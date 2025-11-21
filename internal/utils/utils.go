package utils

import (
	"bytes"
	"encoding/json"
	"log"
	"reflect"

	"hybrid_distributed_store/internal/config"
	"hybrid_distributed_store/internal/interfaces"
)

// Service implements IUtilsSvc.
type Service struct{}

// NewService creates a new UtilsService.
func NewService() interfaces.IUtilsSvc {
	return &Service{}
}

// SeparateHotColdFields splits a map into two maps based on the HotFields configuration.
// Hot data goes to Replication; Cold data goes to Erasure Coding.
func (s *Service) SeparateHotColdFields(data map[string]interface{}) (map[string]interface{}, map[string]interface{}) {
	hotFields := make(map[string]interface{})
	coldFields := make(map[string]interface{})
	
	// Use a set for O(1) lookup
	hotKeysSet := make(map[string]struct{})
	for key := range config.HotFields {
		hotKeysSet[key] = struct{}{}
	}

	for key, value := range data {
		if _, isHot := hotKeysSet[key]; isHot {
			hotFields[key] = value
		} else {
			coldFields[key] = value
		}
	}
	return hotFields, coldFields
}

// MergeHotColdFields combines hot and cold maps back into a single structure.
func (s *Service) MergeHotColdFields(hotFields, coldFields map[string]interface{}) map[string]interface{} {
	merged := make(map[string]interface{}, len(hotFields)+len(coldFields))
	
	// Merge Cold first
	for key, value := range coldFields {
		merged[key] = value
	}
	// Merge Hot second (Hot fields overwrite cold if duplicate keys exist, though unlikely)
	for key, value := range hotFields {
		merged[key] = value
	}
	return merged
}

// Serialize marshals a map into a JSON byte slice.
func (s *Service) Serialize(data map[string]interface{}) ([]byte, error) {
	b, err := json.Marshal(data)
	if err != nil {
		log.Printf("%s[Utils] Serialization failed: %v%s\n", config.Colors["RED"], err, config.Colors["RESET"])
		return nil, err
	}
	return b, nil
}

// Deserialize unmarshals a byte slice into a map.
// It automatically trims null bytes (\x00) which may be present due to EC padding.
func (s *Service) Deserialize(data []byte) (map[string]interface{}, error) {
	// Determine if trimming is needed
	trimmed := bytes.TrimRight(data, "\x00")
	
	if len(trimmed) != len(data) {
		// This log is useful for debugging EC padding issues
		// log.Printf("[Utils] Trimmed padding: %d -> %d bytes\n", len(data), len(trimmed))
	}

	var result map[string]interface{}
	err := json.Unmarshal(trimmed, &result)
	if err != nil {
		log.Printf("%s[Utils] Deserialization failed: %v%s\n", config.Colors["RED"], err, config.Colors["RESET"])
		return nil, err
	}
	return result, nil
}

// MapsAreEqual performs a deep equality check on two maps.
func (s *Service) MapsAreEqual(map1, map2 map[string]interface{}) bool {
	return reflect.DeepEqual(map1, map2)
}