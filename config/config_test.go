package config_test

import (
	"bytes"
	"errors"
	"github.com/cloudfoundry/bosh-azure-storage-cli/config"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Config", func() {

	It("contains account-name and account-name", func() {
		configJson := []byte(`{"account-name": "foo-account-name", 
								"account-key": "bar-account-key", 
								"container-name": "baz-container-name"}`)
		configReader := bytes.NewReader(configJson)

		config, err := config.NewFromReader(configReader)

		Expect(err).ToNot(HaveOccurred())
		Expect(config.AccountName).To(Equal("foo-account-name"))
		Expect(config.AccountKey).To(Equal("bar-account-key"))
		Expect(config.ContainerName).To(Equal("baz-container-name"))
	})

	It("is empty if config cannot be parsed", func() {
		configJson := []byte(`~`)
		configReader := bytes.NewReader(configJson)

		config, err := config.NewFromReader(configReader)

		Expect(err.Error()).To(Equal("invalid character '~' looking for beginning of value"))
		Expect(config.AccountName).Should(BeEmpty())
		Expect(config.AccountKey).Should(BeEmpty())
	})

	Context("when the configuration file cannot be read", func() {
		It("returns an error", func() {
			f := explodingReader{}

			_, err := config.NewFromReader(f)
			Expect(err).To(MatchError("explosion"))
		})
	})

})

type explodingReader struct{}

func (e explodingReader) Read([]byte) (int, error) {
	return 0, errors.New("explosion")
}
