package integration

import (
	"bytes"
	"os"

	"github.com/cloudfoundry/bosh-azure-storage-cli/config"

	. "github.com/onsi/gomega" //nolint:staticcheck
)

func AssertLifecycleWorks(cliPath string, cfg *config.AZStorageConfig) {
	expectedString := GenerateRandomString()
	blobName := GenerateRandomString()

	configPath := MakeConfigFile(cfg)
	defer os.Remove(configPath) //nolint:errcheck

	contentFile := MakeContentFile(expectedString)
	defer os.Remove(contentFile) //nolint:errcheck

	cliSession, err := RunCli(cliPath, configPath, "put", contentFile, blobName)
	Expect(err).ToNot(HaveOccurred())
	Expect(cliSession.ExitCode()).To(BeZero())

	cliSession, err = RunCli(cliPath, configPath, "exists", blobName)
	Expect(err).ToNot(HaveOccurred())
	Expect(cliSession.ExitCode()).To(BeZero())
	Expect(cliSession.Err.Contents()).To(MatchRegexp("File '.*' exists in bucket '.*'"))

	tmpLocalFile, err := os.CreateTemp("", "azure-storage-cli-download")
	Expect(err).ToNot(HaveOccurred())
	err = tmpLocalFile.Close()
	Expect(err).ToNot(HaveOccurred())
	defer os.Remove(tmpLocalFile.Name()) //nolint:errcheck

	cliSession, err = RunCli(cliPath, configPath, "get", blobName, tmpLocalFile.Name())
	Expect(err).ToNot(HaveOccurred())
	Expect(cliSession.ExitCode()).To(BeZero())

	gottenBytes, err := os.ReadFile(tmpLocalFile.Name())
	Expect(err).ToNot(HaveOccurred())
	Expect(string(gottenBytes)).To(Equal(expectedString))

	cliSession, err = RunCli(cliPath, configPath, "delete", blobName)
	Expect(err).ToNot(HaveOccurred())
	Expect(cliSession.ExitCode()).To(BeZero())

	cliSession, err = RunCli(cliPath, configPath, "exists", blobName)
	Expect(err).ToNot(HaveOccurred())
	Expect(cliSession.ExitCode()).To(Equal(3))
	Expect(cliSession.Err.Contents()).To(MatchRegexp("File '.*' does not exist in bucket '.*'"))
}

func AssertOnCliVersion(cliPath string, cfg *config.AZStorageConfig) {
	configPath := MakeConfigFile(cfg)
	defer os.Remove(configPath) //nolint:errcheck

	cliSession, err := RunCli(cliPath, configPath, "-v")
	Expect(err).ToNot(HaveOccurred())
	Expect(cliSession.ExitCode()).To(Equal(0))

	consoleOutput := bytes.NewBuffer(cliSession.Out.Contents()).String()
	Expect(consoleOutput).To(ContainSubstring("version"))
}

func AssertGetNonexistentFails(cliPath string, cfg *config.AZStorageConfig) {
	configPath := MakeConfigFile(cfg)
	defer os.Remove(configPath) //nolint:errcheck

	cliSession, err := RunCli(cliPath, configPath, "get", "non-existent-file", "/dev/null")
	Expect(err).ToNot(HaveOccurred())
	Expect(cliSession.ExitCode()).ToNot(BeZero())
}

func AssertDeleteNonexistentWorks(cliPath string, cfg *config.AZStorageConfig) {
	configPath := MakeConfigFile(cfg)
	defer os.Remove(configPath) //nolint:errcheck

	cliSession, err := RunCli(cliPath, configPath, "delete", "non-existent-file")
	Expect(err).ToNot(HaveOccurred())
	Expect(cliSession.ExitCode()).To(BeZero())
}

func AssertOnSignedURLs(cliPath string, cfg *config.AZStorageConfig) {
	configPath := MakeConfigFile(cfg)
	defer os.Remove(configPath) //nolint:errcheck

	regex := "https://" + cfg.AccountName + ".blob.*/" + cfg.ContainerName + "/some-blob.*"

	cliSession, err := RunCli(cliPath, configPath, "sign", "some-blob", "get", "60s")
	Expect(err).ToNot(HaveOccurred())

	getUrl := bytes.NewBuffer(cliSession.Out.Contents()).String()
	Expect(getUrl).To(MatchRegexp(regex))

	cliSession, err = RunCli(cliPath, configPath, "sign", "some-blob", "put", "60s")
	Expect(err).ToNot(HaveOccurred())

	putUrl := bytes.NewBuffer(cliSession.Out.Contents()).String()
	Expect(putUrl).To(MatchRegexp(regex))
}
