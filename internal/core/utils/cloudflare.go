package utils

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"
)

// CloudflareConfig holds the config needed for cache purge.
type CloudflareConfig struct {
	ZoneID   string
	APIToken string
}

type cfPurgeRequest struct {
	Files []string `json:"files"`
}

type cfPurgeResponse struct {
	Success bool `json:"success"`
	Errors  []struct {
		Message string `json:"message"`
	} `json:"errors"`
}

// PurgeCloudflareCache purges specific URLs from Cloudflare cache (max 30 URLs per request).
func PurgeCloudflareCache(ctx context.Context, cfg CloudflareConfig, urls []string) error {
	if cfg.ZoneID == "" || cfg.APIToken == "" {
		return nil
	}
	if len(urls) == 0 {
		return nil
	}

	const batchSize = 30
	for i := 0; i < len(urls); i += batchSize {
		end := i + batchSize
		if end > len(urls) {
			end = len(urls)
		}
		if err := purgeBatch(ctx, cfg, urls[i:end]); err != nil {
			return err
		}
	}
	return nil
}

func purgeBatch(ctx context.Context, cfg CloudflareConfig, urls []string) error {
	body, err := json.Marshal(cfPurgeRequest{Files: urls})
	if err != nil {
		return fmt.Errorf("marshal purge request: %w", err)
	}

	apiURL := fmt.Sprintf("https://api.cloudflare.com/client/v4/zones/%s/purge_cache", cfg.ZoneID)
	req, err := http.NewRequestWithContext(ctx, "POST", apiURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create purge request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+cfg.APIToken)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("purge request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)

	var purgeResp cfPurgeResponse
	if err := json.Unmarshal(respBody, &purgeResp); err != nil {
		return fmt.Errorf("parse purge response: %w (body: %s)", err, string(respBody))
	}

	if !purgeResp.Success {
		errMsg := "unknown error"
		if len(purgeResp.Errors) > 0 {
			errMsg = purgeResp.Errors[0].Message
		}
		return fmt.Errorf("cloudflare purge failed: %s", errMsg)
	}

	log.Printf("☁️  Cloudflare: Purged %d URL(s)", len(urls))
	return nil
}
