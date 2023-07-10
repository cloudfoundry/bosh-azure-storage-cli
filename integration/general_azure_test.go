package integration_test

import (
	"os"

	"github.com/cloudfoundry/bosh-azure-storage-cli/config"
	"github.com/cloudfoundry/bosh-azure-storage-cli/integration"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("General testing for all Azure regions", func() {
	accountName := os.Getenv("ACCOUNT_NAME")

	accountKey := os.Getenv("ACCOUNT_KEY")

	containerName := os.Getenv("CONTAINER_NAME")


	BeforeEach(func() {
		Expect(accountName).ToNot(BeEmpty(), "ACCOUNT_NAME must be set")
		Expect(accountKey).ToNot(BeEmpty(), "ACCOUNT_KEY must be set")
		Expect(containerName).ToNot(BeEmpty(), "CONTAINER_NAME must be set")
	})

	configurations := []TableEntry{
		Entry("with default config", &config.AZStorageConfig{
			AccountName:   accountName,
			AccountKey:    accountKey,
			ContainerName: containerName,
		}),
	}
	DescribeTable("Blobstore lifecycle works",
		func(cfg *config.AZStorageConfig) { integration.AssertLifecycleWorks(cliPath, cfg) },
		configurations,
	)
	DescribeTable("Invoking `get` on a non-existent-key fails",
		func(cfg *config.AZStorageConfig) { integration.AssertGetNonexistentFails(cliPath, cfg) },
		configurations,
	)
	DescribeTable("Invoking `delete` on a non-existent-key does not fail",
		func(cfg *config.AZStorageConfig) { integration.AssertDeleteNonexistentWorks(cliPath, cfg) },
		configurations,
	)
	DescribeTable("Invoking `sign` returns a signed URL",
		func(cfg *config.AZStorageConfig) { integration.AssertOnSignedURLs(cliPath, cfg) },
		configurations,
	)
	Describe("Invoking `put` with arbitrary upload failures", func() {
		It("returns the appropriate error message", func() {
			cfg := &config.AZStorageConfig{
				AccountName:   accountName,
				AccountKey:    accountKey,
				ContainerName: "not-existing",
			}
			msg := "upload failure"
			integration.AssertOnPutFailures(cliPath, cfg, msg)
		})
	})
})
