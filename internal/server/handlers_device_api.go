package server

import (
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"tronbyt-server/internal/data"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// handleNextApp is the handler for GET /{id}/next.
func (s *Server) handleNextApp(w http.ResponseWriter, r *http.Request) {
	s.metrics.devicePolls.Inc()
	id := r.PathValue("id")

	var device *data.Device
	if d, err := DeviceFromContext(r.Context()); err == nil {
		device = d
	} else if u, err := UserFromContext(r.Context()); err == nil {
		for i := range u.Devices {
			if u.Devices[i].ID == id {
				device = &u.Devices[i]
				break
			}
		}
	} else {
		// Fallback: Fetch from DB directly (No Auth required for device operation)
		d, err := gorm.G[data.Device](s.DB).Preload("Apps", nil).Where("id = ?", id).First(r.Context())
		if err == nil {
			device = &d
		}
	}

	if device == nil {
		http.Error(w, "Device not found", http.StatusNotFound)
		return
	}

	if device.RequireAPIKey {
		if key := extractDeviceKey(r); key == "" || key != device.APIKey {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
	}

	if len(device.Apps) == 0 {
		reloaded, err := gorm.G[data.Device](s.DB).Preload("Apps", nil).Where("id = ?", device.ID).First(r.Context())
		if err == nil {
			device = &reloaded
		}
	}

	user, _ := UserFromContext(r.Context())
	if user == nil {
		owner, err := gorm.G[data.User](s.DB).Where("username = ?", device.Username).First(r.Context())
		if err != nil {
			slog.Error("Failed to find device owner", "username", device.Username, "error", err)
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			return
		}
		user = &owner
	}

	// Update device info if needed
	// We use a transaction with locking to avoid race conditions with other requests
	err := s.DB.Transaction(func(tx *gorm.DB) error {
		// Lock the row to ensure we read the latest state and no one else updates it
		freshDevice, err := gorm.G[data.Device](tx, clause.Locking{Strength: "UPDATE"}).Where("id = ?", device.ID).First(r.Context())
		if err != nil {
			return err
		}

		updated := false
		if freshDevice.Info.ProtocolType != data.ProtocolHTTP {
			slog.Debug("Updating protocol_type to HTTP on /next request", "device", device.ID)
			freshDevice.Info.ProtocolType = data.ProtocolHTTP
			updated = true
		}

		// Check for firmware version header
		if fwVersion := r.Header.Get("X-Firmware-Version"); fwVersion != "" {
			if freshDevice.Info.FirmwareVersion != fwVersion {
				slog.Debug("Updating firmware_version on /next request", "device", device.ID, "version", fwVersion)
				freshDevice.Info.FirmwareVersion = fwVersion
				updated = true
			}
		}

		// Check for firmware type header (e.g. Tronberry, ESP32)
		if fwType := r.Header.Get("X-Firmware-Type"); fwType != "" {
			if freshDevice.Info.FirmwareType != fwType {
				slog.Debug("Updating firmware_type on /next request", "device", device.ID, "type", fwType)
				freshDevice.Info.FirmwareType = fwType
				updated = true
			}
		}

		if updated {
			if _, err := gorm.G[data.Device](tx).Where("id = ?", freshDevice.ID).Update(r.Context(), "info", freshDevice.Info); err != nil {
				return err
			}
			// Update the in-memory device object so subsequent logic uses the new values
			device.Info = freshDevice.Info
		}
		return nil
	})

	if err != nil {
		slog.Error("Failed to update device info transaction", "device", device.ID, "error", err)
	}

	imgData, app, dwell, err := s.GetNextAppImageHTTP(r.Context(), device, user, s.GetBaseURL(r))
	slot, hasSlot := s.takeRotationSlot(device.ID)
	if err != nil {
		// Send default image if error (or not found)
		slog.Error("Failed to get next app image", "device", device.ID, "error", err)
		s.sendDefaultImage(w, r, device)
		return
	}

	// HTTP firmware requests the next image as soon as it starts showing the
	// one from its previous request, so the device is now displaying the app
	// we sent last time, not the one we are about to send. Without a previous
	// send (e.g. after a server restart), fall back to the app being sent.
	if app != nil {
		s.httpLastSentMu.Lock()
		if s.httpLastSentApp == nil {
			s.httpLastSentApp = make(map[string]string)
		}
		displaying, hadPrevious := s.httpLastSentApp[device.ID]
		s.httpLastSentApp[device.ID] = app.Iname
		s.httpLastSentMu.Unlock()
		if !hadPrevious {
			displaying = app.Iname
		}

		// Firmware that knows what is on screen (e.g. while it shows cached
		// images during a slow download) reports it; prefer that to the guess.
		fromReport := false
		if reported := r.Header.Get("Tronbyt-Displaying"); reported != "" {
			for _, a := range device.Apps {
				if a.Iname == reported {
					displaying = reported
					fromReport = true
					break
				}
			}
		}

		// Remember where and since when, so "Now Showing" can follow a device
		// that keeps rotating from its image cache between requests.
		anchor := httpDisplayAnchor{Iname: displaying, Index: -1, At: time.Now()}
		if fromReport {
			if idx, err := strconv.Atoi(r.Header.Get("Tronbyt-Displaying-Index")); err == nil {
				anchor.Index = idx
			}
			if since, err := strconv.ParseInt(r.Header.Get("Tronbyt-Displaying-Since"), 10, 64); err == nil &&
				plausibleDisplayStart(time.UnixMilli(since), anchor.At) {
				// Absolute start from the device's synced clock: exact, however
				// long the request took to arrive.
				anchor.At = time.UnixMilli(since)
			} else if ms, err := strconv.Atoi(r.Header.Get("Tronbyt-Displaying-Ms")); err == nil && ms > 0 {
				anchor.At = anchor.At.Add(-time.Duration(ms) * time.Millisecond)
			}
			anchor.Caches = r.Header.Get("Tronbyt-Image-Cache") == "1"
		}
		s.setHTTPDisplayAnchor(device.ID, anchor)
		s.scheduleDisplayUpdate(device.ID, user.Username)

		if _, err := gorm.G[data.Device](s.DB).Where("id = ?", device.ID).Update(r.Context(), "displaying_app", displaying); err != nil {
			slog.Error("Failed to update displaying_app for HTTP device", "device", device.ID, "error", err)
		}
	}

	// Send Headers
	w.Header().Set("Content-Type", "image/webp")
	w.Header().Set("Cache-Control", "public, max-age=0, must-revalidate")

	s.sendPendingHTTPDeviceHeaders(w, r.Context(), device)

	// Determine Brightness
	brightness := device.GetEffectiveBrightness()
	w.Header().Set("Tronbyt-Brightness", fmt.Sprintf("%d", brightness))

	w.Header().Set("Tronbyt-Dwell-Secs", fmt.Sprintf("%d", dwell))

	// Rotation position, so devices can cache images in rotation order.
	// Animations split into several chunks are left out: their chunks would
	// share one position.
	if app != nil && hasSlot && len(s.getAppChunks(device.ID, app)) <= 1 {
		w.Header().Set("Tronbyt-App-Id", app.Iname)
		w.Header().Set("Tronbyt-App-Index", fmt.Sprintf("%d", slot.Index))
		w.Header().Set("Tronbyt-App-Count", fmt.Sprintf("%d", slot.Count))
	}

	if _, err := w.Write(imgData); err != nil {
		slog.Error("Failed to write image data to response", "error", err)
		// Log error, but can't change HTTP status after writing headers.
	}
}
