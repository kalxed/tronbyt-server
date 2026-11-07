package server

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"time"

	"tronbyt-server/internal/data"
	"tronbyt-server/web"

	securejoin "github.com/cyphar/filepath-securejoin"
	"gorm.io/gorm"
)

func (s *Server) hasEphemeralPushed(deviceID string) bool {
	pushedDir := filepath.Join(s.DataDir, "webp", deviceID, "pushed")
	entries, err := os.ReadDir(pushedDir)
	if err != nil {
		return false
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "__") && strings.HasSuffix(entry.Name(), ".webp") && !entry.IsDir() {
			return true
		}
	}
	return false
}

// rotationSlot is the position of an app in a device's normal rotation.
// It is sent to HTTP devices so they can cache images in rotation order.
// Count is the number of positions currently in the rotation (disabled,
// scheduled-out and empty apps excluded), Index the position among them.
type rotationSlot struct {
	Index int
	Count int
}

// setRotationSlot records where the image just chosen for a device sits in
// its rotation. A nil slot means the image is not part of the normal
// rotation (pushed image, pinned or night mode app, default image).
func (s *Server) setRotationSlot(deviceID string, slot *rotationSlot) {
	s.rotationSlotsMu.Lock()
	defer s.rotationSlotsMu.Unlock()
	if s.rotationSlots == nil {
		s.rotationSlots = make(map[string]rotationSlot)
	}
	if slot == nil {
		delete(s.rotationSlots, deviceID)
		return
	}
	s.rotationSlots[deviceID] = *slot
}

// takeRotationSlot returns and clears the slot recorded by setRotationSlot.
func (s *Server) takeRotationSlot(deviceID string) (rotationSlot, bool) {
	s.rotationSlotsMu.Lock()
	defer s.rotationSlotsMu.Unlock()
	slot, ok := s.rotationSlots[deviceID]
	delete(s.rotationSlots, deviceID)
	return slot, ok
}

// httpDisplayAnchor is the app an HTTP device last reported on screen.
type httpDisplayAnchor struct {
	Iname  string
	Index  int       // position among the active apps, -1 if unknown
	At     time.Time // when it appeared on screen
	Caches bool      // the device keeps rotating from its image cache
}

func (s *Server) setHTTPDisplayAnchor(deviceID string, a httpDisplayAnchor) {
	s.httpDisplayMu.Lock()
	defer s.httpDisplayMu.Unlock()
	if s.httpDisplay == nil {
		s.httpDisplay = make(map[string]httpDisplayAnchor)
	}
	s.httpDisplay[deviceID] = a
}

// maxDisplayPrediction bounds how long "Now Showing" keeps advancing without
// hearing from the device, which may have been switched off.
const maxDisplayPrediction = 10 * time.Minute

// plausibleDisplayStart reports whether a start time sent by a device is
// usable: not in the future and within the prediction window (a device clock
// that is not synced yet sends nonsense).
func plausibleDisplayStart(start, now time.Time) bool {
	return !start.After(now.Add(2*time.Second)) && now.Sub(start) <= maxDisplayPrediction
}

// predictDisplay returns the app a caching HTTP device shows now and how long
// until it changes. Starting at the app it last reported, it walks the active
// rotation with each app's dwell time, as the device does while it plays its
// image cache.
func (s *Server) predictDisplay(device *data.Device) (string, time.Duration, bool) {
	s.httpDisplayMu.Lock()
	a, ok := s.httpDisplay[device.ID]
	s.httpDisplayMu.Unlock()
	if !ok || !a.Caches {
		return "", 0, false
	}
	elapsed := time.Since(a.At)
	if elapsed < 0 {
		elapsed = 0
	}
	if elapsed > maxDisplayPrediction {
		return "", 0, false
	}

	apps := slices.Clone(device.Apps)
	sort.Slice(apps, func(i, j int) bool {
		return apps[i].Order < apps[j].Order
	})
	expanded := createExpandedAppsList(device, apps)
	var active []*data.App
	for i, app := range expanded {
		if shouldDisplayAt(device, expanded, i) && !app.EmptyLastRender {
			active = append(active, app)
		}
	}

	pos := -1
	if a.Index >= 0 && a.Index < len(active) && active[a.Index].Iname == a.Iname {
		pos = a.Index
	} else {
		for i, app := range active {
			if app.Iname == a.Iname {
				pos = i
				break
			}
		}
	}
	if pos < 0 {
		return "", 0, false
	}

	for {
		dwell := time.Duration(max(1, device.GetEffectiveDwellTime(active[pos]))) * time.Second
		if elapsed < dwell {
			return active[pos].Iname, dwell - elapsed, true
		}
		elapsed -= dwell
		pos = (pos + 1) % len(active)
	}
}

// scheduleDisplayUpdate tells the dashboard when a caching HTTP device moves
// on to its next app, since no request marks that moment. Each call replaces
// the device's pending timer; the timer reschedules itself until prediction
// stops (no cache, or no report for maxDisplayPrediction).
func (s *Server) scheduleDisplayUpdate(deviceID, username string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	device, err := gorm.G[data.Device](s.DB).Preload("Apps", nil).Where("id = ?", deviceID).First(ctx)
	cancel()
	var next time.Duration
	ok := false
	if err == nil {
		_, next, ok = s.predictDisplay(&device)
	}

	s.httpDisplayMu.Lock()
	defer s.httpDisplayMu.Unlock()
	if s.httpDisplayTimers == nil {
		s.httpDisplayTimers = make(map[string]*time.Timer)
	}
	if t := s.httpDisplayTimers[deviceID]; t != nil {
		t.Stop()
		delete(s.httpDisplayTimers, deviceID)
	}
	if !ok {
		return
	}
	// A little after the change, so the dashboard's fetch sees the new app.
	s.httpDisplayTimers[deviceID] = time.AfterFunc(next+50*time.Millisecond, func() {
		s.notifyDashboard(username, WSEvent{Type: "image_updated", DeviceID: deviceID})
		s.scheduleDisplayUpdate(deviceID, username)
	})
}

// GetNextAppImageHTTP picks what a device should display next, with support for
// multi-chunk animations (serving sequential chunks across repeated /next calls
// with custom dwell times).
// Returns (imgData, app, dwellSecs, error).
func (s *Server) GetNextAppImageHTTP(ctx context.Context, device *data.Device, user *data.User, baseURL string) ([]byte, *data.App, int, error) {
	// If an ephemeral pushed image is waiting, reset any chunk tracking and serve it immediately.
	if s.hasEphemeralPushed(device.ID) {
		s.deviceChunkMu.Lock()
		delete(s.deviceCurrentChunkApp, device.ID)
		delete(s.deviceChunkIndices, device.ID)
		s.deviceChunkMu.Unlock()

		imgData, app, err := s.GetNextAppImage(ctx, device, user, baseURL)
		dwell := device.GetEffectiveDwellTime(app)
		return imgData, app, dwell, err
	}

	// Check if this device is currently traversing chunks of an active multi-chunk app.
	s.deviceChunkMu.Lock()
	currIname, hasActive := s.deviceCurrentChunkApp[device.ID]
	currIdx := s.deviceChunkIndices[device.ID]
	s.deviceChunkMu.Unlock()

	if hasActive {
		// Find the active app in device.Apps
		var activeApp *data.App
		for _, a := range device.Apps {
			if a.Iname == currIname {
				activeApp = a
				break
			}
		}

		if activeApp != nil && activeApp.Enabled && IsAppScheduleActive(activeApp, device) {
			chunks := s.getAppChunks(device.ID, activeApp)
			maxChunkSize := s.GetMaxChunkSizeForDevice(device)
			if len(chunks) > 1 && currIdx < len(chunks) {
				chunk := chunks[currIdx]
				if maxChunkSize > 0 && len(chunk.WebPBytes) > maxChunkSize {
					slog.Warn("HTTP chunk exceeds device max buffer size, aborting multi-chunk sequence", "device", device.ID, "app", activeApp.Iname, "chunk_size", len(chunk.WebPBytes), "max", maxChunkSize)
					s.deviceChunkMu.Lock()
					delete(s.deviceCurrentChunkApp, device.ID)
					delete(s.deviceChunkIndices, device.ID)
					s.deviceChunkMu.Unlock()
				} else {
					dwell := max(1, chunk.DurationMs/1000)

					s.deviceChunkMu.Lock()
					nextIdx := currIdx + 1
					if nextIdx >= len(chunks) {
						// Last chunk reached; clear tracking so the next /next request rotates to the next app
						delete(s.deviceCurrentChunkApp, device.ID)
						delete(s.deviceChunkIndices, device.ID)
					} else {
						s.deviceChunkIndices[device.ID] = nextIdx
					}
					s.deviceChunkMu.Unlock()

					now := time.Now()
					_, _ = gorm.G[data.Device](s.DB).Where("id = ?", device.ID).Select("LastSeen").Updates(ctx, data.Device{LastSeen: &now})
					device.LastSeen = &now

					return chunk.WebPBytes, activeApp, dwell, nil
				}
			}
		}

		// App missing, disabled, schedule inactive, or chunks invalid: reset tracking
		s.deviceChunkMu.Lock()
		delete(s.deviceCurrentChunkApp, device.ID)
		delete(s.deviceChunkIndices, device.ID)
		s.deviceChunkMu.Unlock()
	}

	// Normal rotation to next app
	imgData, app, err := s.GetNextAppImage(ctx, device, user, baseURL)
	if err != nil || app == nil {
		dwell := device.GetEffectiveDwellTime(app)
		return imgData, app, dwell, err
	}

	chunks := s.getAppChunks(device.ID, app)
	if len(chunks) > 1 {
		s.deviceChunkMu.Lock()
		s.deviceCurrentChunkApp[device.ID] = app.Iname
		s.deviceChunkIndices[device.ID] = 1 // chunk 0 is returned now; chunk 1 is next
		s.deviceChunkMu.Unlock()
		chunk0 := chunks[0]
		dwell := max(1, chunk0.DurationMs/1000)
		return chunk0.WebPBytes, app, dwell, nil
	}

	dwell := device.GetEffectiveDwellTime(app)
	return imgData, app, dwell, nil
}

// GetNextAppImage picks what a device should display next.
//
// baseURL is this server's address as seen by the requesting device; it is
// used to draw a setup screen when the device has no apps, and may be empty
// when no request context is available (the placeholder is used instead).
func (s *Server) GetNextAppImage(ctx context.Context, device *data.Device, user *data.User, baseURL string) ([]byte, *data.App, error) {
	// Only the normal rotation below records a slot.
	s.setRotationSlot(device.ID, nil)

	// 1. Check Pushed Ephemeral Images (__*)
	// Serve the oldest and delete only that one. Anonymous pushes accumulate
	// unbounded; callers should use coalesceID to limit queue depth.
	pushedDir := filepath.Join(s.DataDir, "webp", device.ID, "pushed")
	if entries, err := os.ReadDir(pushedDir); err == nil {
		var ephemeral []os.DirEntry
		for _, entry := range entries {
			if strings.HasPrefix(entry.Name(), "__") && strings.HasSuffix(entry.Name(), ".webp") && !entry.IsDir() {
				ephemeral = append(ephemeral, entry)
			}
		}
		if len(ephemeral) > 0 {
			// Sort ascending by name (lower timestamp = older)
			sort.Slice(ephemeral, func(i, j int) bool {
				return ephemeral[i].Name() < ephemeral[j].Name()
			})

			for _, entry := range ephemeral {
				entryPath := filepath.Join(pushedDir, entry.Name())
				imgData, err := os.ReadFile(entryPath)
				if err == nil {
					// Delete only the one served; coalesceID handles dedup at write time
					if err := os.Remove(entryPath); err != nil {
						slog.Warn("Failed to remove ephemeral image", "path", entryPath, "error", err)
					}
					return imgData, nil, nil
				}
				// If reading failed, clean it up and try the next one
				slog.Warn("Failed to read ephemeral image, cleaning up", "path", entryPath, "error", err)
				if err := os.Remove(entryPath); err != nil {
					slog.Warn("Failed to remove broken ephemeral image", "path", entryPath, "error", err)
				}
			}
		}
	}

	// Helper to return default image
	getDefaultImage := func() ([]byte, *data.App, error) {
		data, err := web.Assets.ReadFile("static/images/default.webp")
		if err != nil {
			return nil, nil, fmt.Errorf("failed to read default image: %w", err)
		}
		return data, nil, nil
	}

	// 2. Apps Check
	if len(device.Apps) == 0 {
		// Nothing installed yet: show where to install something rather than
		// a placeholder that gives no way to act on it.
		if baseURL != "" {
			width, height := device.Type.DisplaySize()
			if img, err := renderSetupImage(ctx, width, height, baseURL); err == nil {
				return img, nil, nil
			} else {
				slog.Warn("Could not render setup image, falling back to default",
					"device", device.ID, "error", err)
			}
		}
		slog.Debug("No apps on device, returning default image", "device", device.ID)
		return getDefaultImage()
	}

	// 3. Brightness Check
	if device.GetEffectiveBrightness() == 0 {
		slog.Debug("Brightness is 0, returning default image")
		return getDefaultImage()
	}

	// 4. Rotation Logic
	app, nextIndex, err := s.determineNextApp(ctx, device, user)
	if err != nil || app == nil {
		slog.Debug("No valid app found (e.g. all disabled or scheduled out), returning default image", "device", device.ID, "error", err)
		return getDefaultImage()
	}

	// 5. Save State
	now := time.Now()
	deviceUpdates := data.Device{
		LastSeen: &now,
	}

	q := gorm.G[data.Device](s.DB).Where("id = ?", device.ID)
	hasIndexUpdate := false
	if nextIndex != device.LastAppIndex {
		deviceUpdates.LastAppIndex = nextIndex
		q = q.Select("LastSeen", "LastAppIndex")
		hasIndexUpdate = true
	} else {
		q = q.Select("LastSeen")
	}

	if _, err := q.Updates(ctx, deviceUpdates); err != nil {
		slog.Error("Failed to update device state (last_app_index/last_seen)", "error", err)
	} else {
		if hasIndexUpdate {
			device.LastAppIndex = nextIndex
		}
		device.LastSeen = &now
	}

	// Notify Dashboard that the device has updated (new app or new render)
	// For WS devices, we wait for the ACK to send this event to avoid "future" previews.
	if user != nil && device.Info.ProtocolType != data.ProtocolWS {
		s.notifyDashboard(user.Username, WSEvent{Type: "image_updated", DeviceID: device.ID})
	}

	// Trigger every_display notification sound for HTTP polling devices
	if device.Info.ProtocolType != data.ProtocolWS && app != nil && app.NotificationSound != nil && *app.NotificationSound != "" && *app.NotificationSound != "none" && app.NotificationSoundTrigger == "every_display" {
		if device.HasSpeaker() && !device.IsMuted() {
			baseURLToUse := s.GetDeviceBaseURL(device)
			if baseURLToUse == "" || (!strings.Contains(baseURLToUse, ":") && strings.Contains(baseURL, ":")) {
				baseURLToUse = baseURL
			}
			_ = s.TriggerDeviceSound(ctx, device, *app.NotificationSound, baseURLToUse)
		}
	}

	// 6. Return Image
	var webpPath string

	deviceWebpDir, err := s.ensureDeviceImageDir(device.ID)
	if err != nil {
		slog.Error("Failed to get device webp directory for next app image", "device_id", device.ID, "error", err)
		return getDefaultImage()
	}

	webpPath = s.getAppWebpPath(deviceWebpDir, app)

	data, err := os.ReadFile(webpPath)
	// If reading the specific app image fails, fall back to default
	if err != nil {
		slog.Error("Failed to read app image", "path", webpPath, "error", err)
		return getDefaultImage()
	}

	// Guard against oversized images for the device (e.g. cached files from before multi-chunk capping)
	maxChunkSize := s.GetMaxChunkSizeForDevice(device)
	if maxChunkSize > 0 && len(data) > maxChunkSize {
		// Attempt fallback to chunk 0 if multi-chunk exists and fits
		if chunks := s.getAppChunks(device.ID, app); len(chunks) > 0 && len(chunks[0].WebPBytes) <= maxChunkSize {
			slog.Warn("Primary webp exceeds device max chunk size; serving chunk 0 instead",
				"device", device.ID, "app", app.Iname, "primary_size", len(data), "chunk0_size", len(chunks[0].WebPBytes), "max", maxChunkSize)
			return chunks[0].WebPBytes, app, nil
		}
		slog.Error("App image exceeds device max buffer size and no valid chunk found",
			"device", device.ID, "app", app.Iname, "size", len(data), "max", maxChunkSize)
		return getDefaultImage()
	}

	return data, app, err
}

func (s *Server) GetCurrentAppImage(ctx context.Context, device *data.Device) ([]byte, *data.App, error) {
	// Re-fetch device with Apps if missing
	if len(device.Apps) == 0 {
		reloaded, err := gorm.G[data.Device](s.DB).Preload("Apps", nil).Where("id = ?", device.ID).First(ctx)
		if err == nil {
			*device = reloaded
		}
	}

	displayingApp := device.DisplayingApp
	// Caching HTTP devices keep rotating between requests: follow along.
	if predicted, _, ok := s.predictDisplay(device); ok {
		displayingApp = &predicted
	}

	// Priority 1: Check DisplayingApp (Real-time confirmation from WS devices)
	if displayingApp != nil && *displayingApp != "" {
		targetIname := *displayingApp
		// slog.Debug("Checking DisplayingApp", "iname", targetIname)
		// Find app in device.Apps
		for i := range device.Apps {
			if device.Apps[i].Iname == targetIname {
				app := device.Apps[i]

				// Generate path
				deviceWebpDir, err := s.ensureDeviceImageDir(device.ID)
				if err != nil {
					slog.Warn("Failed to get device webp directory", "device_id", device.ID, "error", err)
					break
				}
				webpPath := s.getAppWebpPath(deviceWebpDir, app)

				// Check if file exists to be safe
				if _, err := os.Stat(webpPath); err == nil {
					data, err := os.ReadFile(webpPath)
					return data, app, err
				} else {
					slog.Warn("DisplayingApp file missing, falling back", "path", webpPath)
				}
				break // Valid app but missing file, fallthrough to legacy logic
			}
		}
	}

	// Priority 2: Fallback to LastAppIndex (Legacy/HTTP devices)
	apps := slices.Clone(device.Apps)
	sort.Slice(apps, func(i, j int) bool {
		return apps[i].Order < apps[j].Order
	})
	expanded := createExpandedAppsList(device, apps)

	if len(expanded) == 0 {
		return nil, nil, fmt.Errorf("no apps")
	}

	idx := device.LastAppIndex
	if idx >= len(expanded) {
		idx = 0
	}

	app := expanded[idx]

	// Return image
	var webpPath string

	deviceWebpDir, err := s.ensureDeviceImageDir(device.ID)
	if err != nil {
		slog.Error("Failed to get device webp directory for current app image", "device_id", device.ID, "error", err)
		return nil, nil, fmt.Errorf("failed to get device webp directory: %w", err)
	}

	webpPath = s.getAppWebpPath(deviceWebpDir, app)

	data, err := os.ReadFile(webpPath)
	return data, app, err
}

func (s *Server) determineNextApp(ctx context.Context, device *data.Device, user *data.User) (*data.App, int, error) {
	// 1. Night Mode Logic (Highest Priority)
	nightModeActive := device.GetNightModeIsActive()
	if nightModeActive && device.NightModeApp != "" {
		nightIname := device.NightModeApp
		for i := range device.Apps {
			if device.Apps[i].Iname == nightIname {
				app := device.Apps[i]
				// Found Night Mode app, check if it's renderable before returning
				if s.possiblyRender(ctx, app, device, user) && !app.EmptyLastRender {
					return app, device.LastAppIndex, nil
				}
				// Stop if context was canceled
				if ctx.Err() != nil {
					slog.Debug("Context canceled during night mode app render", "device", device.ID)
					return nil, 0, ctx.Err()
				}
				slog.Warn("Night Mode App failed to render, falling back", "app", nightIname, "device", device.ID)
				break // Stop looking for night app and fall through
			}
		}
	}

	// 2. Sticky Pin Logic
	if device.PinnedApp != nil && *device.PinnedApp != "" {
		pinnedIname := *device.PinnedApp
		foundPinned := false
		for i := range device.Apps {
			if device.Apps[i].Iname == pinnedIname {
				foundPinned = true
				app := device.Apps[i]
				// Found pinned app, check renderability
				if s.possiblyRender(ctx, app, device, user) && !app.EmptyLastRender {
					return app, device.LastAppIndex, nil
				}
				// Stop if context was canceled
				if ctx.Err() != nil {
					slog.Debug("Context canceled during pinned app render", "device", device.ID)
					return nil, 0, ctx.Err()
				}
				slog.Warn("Pinned App failed to render, falling back", "app", pinnedIname, "device", device.ID)
				break // Found but failed, fall through to normal rotation without unpinning
			}
		}

		if !foundPinned {
			// Pinned app not found (e.g. delivered/deleted), clear pin and continue
			slog.Warn("Pinned app not found on device, clearing pin", "device", device.ID, "app", pinnedIname)
			if _, err := gorm.G[data.Device](s.DB).Where("id = ?", device.ID).Update(ctx, "pinned_app", nil); err != nil {
				slog.Error("Failed to clear invalid pinned app", "device", device.ID, "error", err)
			} else {
				device.PinnedApp = nil
			}
		}
	}

	// Sort Apps
	apps := slices.Clone(device.Apps)
	sort.Slice(apps, func(i, j int) bool {
		return apps[i].Order < apps[j].Order
	})

	// Create Expanded List (with Interstitials)
	expanded := createExpandedAppsList(device, apps)

	if len(expanded) == 0 {
		return nil, 0, nil
	}

	lastIndex := device.LastAppIndex

	// Loop to find next valid app
	for i := 0; i < len(expanded)*2; i++ {
		nextIndex := (lastIndex + 1) % len(expanded)

		candidate := expanded[nextIndex]

		if shouldDisplayAt(device, expanded, nextIndex) {
			if s.possiblyRender(ctx, candidate, device, user) && !candidate.EmptyLastRender {
				slot := activeRotationSlot(device, expanded, nextIndex)
				s.setRotationSlot(device.ID, &slot)
				return candidate, nextIndex, nil
			}
			// Stop trying more apps if context was canceled (e.g., request timed out)
			if ctx.Err() != nil {
				slog.Debug("Context canceled during app iteration, stopping", "device", device.ID)
				return nil, 0, ctx.Err()
			}
		}

		lastIndex = nextIndex
	}

	return nil, 0, nil
}

// shouldDisplayAt reports whether the app at position i of the expanded list
// is eligible for the rotation (enabled, schedule active, interstitial
// rules). It does not render and ignores EmptyLastRender.
func shouldDisplayAt(device *data.Device, expanded []*data.App, i int) bool {
	candidate := expanded[i]
	isInterstitialPos := device.InterstitialEnabled && i%2 == 1

	shouldDisplay := false
	if isInterstitialPos {
		shouldDisplay = true
		// Interstitial Logic: Skip if previous regular app (at index-1) is skipped
		// Note: expanded list is always [App, Interstitial, App, Interstitial...]
		// So an interstitial at index i corresponds to App at i-1.
		if i > 0 {
			prevApp := expanded[i-1]
			prevActive := prevApp.Enabled && IsAppScheduleActive(prevApp, device)
			if !prevActive {
				shouldDisplay = false
			}
		}
	} else if candidate.Enabled && IsAppScheduleActive(candidate, device) {
		shouldDisplay = true
	}

	if device.InterstitialApp != nil && *device.InterstitialApp == candidate.Iname && !isInterstitialPos {
		if !candidate.Enabled {
			shouldDisplay = false
		}
	}
	return shouldDisplay
}

// activeRotationSlot returns the position of expanded[index] among the
// positions that currently take part in the rotation (eligible and with a
// non-empty last render), and how many there are. The app at index is the
// one just chosen, so it always counts as active.
func activeRotationSlot(device *data.Device, expanded []*data.App, index int) rotationSlot {
	slot := rotationSlot{}
	for i, app := range expanded {
		active := i == index || (shouldDisplayAt(device, expanded, i) && !app.EmptyLastRender)
		if !active {
			continue
		}
		if i < index {
			slot.Index++
		}
		slot.Count++
	}
	return slot
}

func createExpandedAppsList(device *data.Device, apps []*data.App) []*data.App {
	if !device.InterstitialEnabled || device.InterstitialApp == nil {
		return apps
	}

	interstitialIname := *device.InterstitialApp
	var interstitialApp *data.App

	// Find interstitial app object
	for i := range apps {
		if apps[i].Iname == interstitialIname {
			interstitialApp = apps[i]
			break
		}
	}

	if interstitialApp == nil {
		return apps
	}

	expanded := make([]*data.App, 0, len(device.Apps)*10) // Pre-allocate to a reasonable size
	for i, app := range apps {
		expanded = append(expanded, app)
		// Add interstitial after each regular app, except the last one
		if i < len(apps)-1 {
			expanded = append(expanded, interstitialApp)
		}
	}
	return expanded
}

// getAppWebpPath generates the file path for an app's WebP image.
// It handles both pushed apps (stored in pushed/ subdirectory) and regular apps.
func (s *Server) getAppWebpPath(deviceWebpDir string, app *data.App) string {
	if app.Pushed && app.Path != nil && strings.HasPrefix(*app.Path, "pushed:") {
		installID := strings.TrimPrefix(*app.Path, "pushed:")
		path, err := securejoin.SecureJoin(filepath.Join(deviceWebpDir, "pushed"), installID+".webp")
		if err != nil {
			slog.Error("Failed to resolve pushed image path", "error", err)
			return ""
		}
		return path
	}
	appBasename := fmt.Sprintf("%s-%s", app.Name, app.Iname)
	return filepath.Join(deviceWebpDir, fmt.Sprintf("%s.webp", appBasename))
}
