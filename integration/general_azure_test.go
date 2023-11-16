package integration_test

import (
	"bytes"
	"os"

	"github.com/cloudfoundry/bosh-azure-storage-cli/config"
	"github.com/cloudfoundry/bosh-azure-storage-cli/integration"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("General testing for all Azure regions", func() {

	BeforeEach(func() {
		Expect(os.Getenv("ACCOUNT_NAME")).ToNot(BeEmpty(), "ACCOUNT_NAME must be set")
		Expect(os.Getenv("ACCOUNT_KEY")).ToNot(BeEmpty(), "ACCOUNT_KEY must be set")
		Expect(os.Getenv("CONTAINER_NAME")).ToNot(BeEmpty(), "CONTAINER_NAME must be set")
	})

	configurations := []TableEntry{
		Entry("with default config", defaultConfig()),
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
	Describe("Invoking `put`", func() {
		var blobName string
		var configPath string
		var contentFile string

		BeforeEach(func() {
			blobName = integration.GenerateRandomString()
			configPath = integration.MakeConfigFile(defaultConfig())
			contentFile = integration.MakeContentFile("foo")
		})

		AfterEach(func() {
			defer func() { _ = os.Remove(configPath) }()
			defer func() { _ = os.Remove(contentFile) }()
		})

		It("uploads a file", func() {
			defer func() {
				cliSession, err := integration.RunCli(cliPath, configPath, "delete", blobName)
				Expect(err).ToNot(HaveOccurred())
				Expect(cliSession.ExitCode()).To(BeZero())
			}()

			cliSession, err := integration.RunCli(cliPath, configPath, "put", contentFile, blobName)
			Expect(err).ToNot(HaveOccurred())
			Expect(cliSession.ExitCode()).To(BeZero())

			cliSession, err = integration.RunCli(cliPath, configPath, "exists", blobName)
			Expect(err).ToNot(HaveOccurred())
			Expect(cliSession.ExitCode()).To(BeZero())
			Expect(string(cliSession.Err.Contents())).To(MatchRegexp("File '" + blobName + "' exists in bucket 'test-container'"))
		})

		It("overwrites an existing file", func() {
			defer func() {
				cliSession, err := integration.RunCli(cliPath, configPath, "delete", blobName)
				Expect(err).ToNot(HaveOccurred())
				Expect(cliSession.ExitCode()).To(BeZero())
			}()

			tmpLocalFile, _ := os.CreateTemp("", "azure-storage-cli-download")
			tmpLocalFile.Close()
			defer func() { _ = os.Remove(tmpLocalFile.Name()) }()

			contentFile = integration.MakeContentFile("initial content")
			cliSession, err := integration.RunCli(cliPath, configPath, "put", contentFile, blobName)
			Expect(err).ToNot(HaveOccurred())
			Expect(cliSession.ExitCode()).To(BeZero())

			cliSession, err = integration.RunCli(cliPath, configPath, "get", blobName, tmpLocalFile.Name())
			Expect(err).ToNot(HaveOccurred())
			Expect(cliSession.ExitCode()).To(BeZero())

			gottenBytes, _ := os.ReadFile(tmpLocalFile.Name())
			Expect(string(gottenBytes)).To(Equal("initial content"))

			contentFile = integration.MakeContentFile("updated content")
			cliSession, err = integration.RunCli(cliPath, configPath, "put", contentFile, blobName)
			Expect(err).ToNot(HaveOccurred())
			Expect(cliSession.ExitCode()).To(BeZero())

			cliSession, err = integration.RunCli(cliPath, configPath, "get", blobName, tmpLocalFile.Name())
			Expect(err).ToNot(HaveOccurred())
			Expect(cliSession.ExitCode()).To(BeZero())

			gottenBytes, _ = os.ReadFile(tmpLocalFile.Name())
			Expect(string(gottenBytes)).To(Equal("updated content"))
		})

		It("returns the appropriate error message", func() {
			cfg := &config.AZStorageConfig{
				AccountName:   os.Getenv("ACCOUNT_NAME"),
				AccountKey:    os.Getenv("ACCOUNT_KEY"),
				ContainerName: "not-existing",
			}

			configPath = integration.MakeConfigFile(cfg)

			cliSession, err := integration.RunCli(cliPath, configPath, "put", contentFile, blobName)
			Expect(err).ToNot(HaveOccurred())
			Expect(cliSession.ExitCode()).To(Equal(1))

			consoleOutput := bytes.NewBuffer(cliSession.Err.Contents()).String()
			Expect(consoleOutput).To(ContainSubstring("upload failure"))
		})
	})
	Describe("Invoking `-v`", func() {
		It("returns the cli version", func() {
			integration.AssertOnCliVersion(cliPath, defaultConfig())
		})
	})
})

func defaultConfig() *config.AZStorageConfig {

	return &config.AZStorageConfig{
		AccountName:   os.Getenv("ACCOUNT_NAME"),
		AccountKey:    os.Getenv("ACCOUNT_KEY"),
		ContainerName: os.Getenv("CONTAINER_NAME"),
	}
}
