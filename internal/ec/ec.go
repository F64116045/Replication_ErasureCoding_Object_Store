package ec

import (
	"log"
	"runtime"

	"github.com/klauspost/cpuid/v2"
	"github.com/klauspost/reedsolomon"
	"hybrid_distributed_store/internal/config"
	"hybrid_distributed_store/internal/interfaces"
)

// Service implements the IEcDriver interface using the Klauspost Reed-Solomon library.
// It handles splitting data into shards, encoding parity, and reconstructing missing data.
type Service struct {
	driver reedsolomon.Encoder
}

// NewService initializes the Reed-Solomon encoder and checks for CPU hardware acceleration.
// It returns an instance of interfaces.IEcDriver.
func NewService() interfaces.IEcDriver {
	log.Println("--- Initializing EC Driver (Reed-Solomon) ---")

	// Initialize RS encoder with K data shards and M parity shards.
	// Enable AutoGoroutines to utilize multi-core CPUs for encoding tasks.
	enc, err := reedsolomon.New(config.K, config.M, reedsolomon.WithAutoGoroutines(runtime.NumCPU()))
	if err != nil {
		log.Fatalf("Failed to create Reed-Solomon encoder: %v", err)
	}

	// Check for hardware acceleration support (AVX512 / AVX2)
	if cpuid.CPU.Supports(cpuid.AVX512F, cpuid.AVX512BW, cpuid.AVX512DQ, cpuid.AVX512VL) {
		log.Printf("%s[EC Driver] CPU supports AVX512. High performance mode enabled.%s\n", config.Colors["GREEN"], config.Colors["RESET"])
	} else if cpuid.CPU.Supports(cpuid.AVX2) {
		log.Printf("%s[EC Driver] CPU supports AVX2. Performance mode enabled.%s\n", config.Colors["GREEN"], config.Colors["RESET"])
	} else {
		log.Printf("%s[EC Driver] WARNING: CPU does not support AVX2/AVX512. Performance may be degraded.%s\n", config.Colors["YELLOW"], config.Colors["RESET"])
	}

	log.Printf("%sEC Driver loaded (k=%d, m=%d). Using %d CPU cores.%s\n",
		config.Colors["CYAN"], config.K, config.M, runtime.NumCPU(), config.Colors["RESET"])

	return &Service{driver: enc}
}

// Split divides the data into data shards.
// The number of shards is determined by config.K + config.M.
func (s *Service) Split(data []byte) ([][]byte, error) {
	return s.driver.Split(data)
}

// Encode generates parity shards for the given set of shards.
// It modifies the shards slice in place to populate the parity shards.
func (s *Service) Encode(shards [][]byte) error {
	return s.driver.Encode(shards)
}

// Reconstruct repairs missing shards (nil in the slice) if enough shards are present.
func (s *Service) Reconstruct(shards [][]byte) error {
	return s.driver.Reconstruct(shards)
}