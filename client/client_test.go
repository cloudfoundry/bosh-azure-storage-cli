package client_test

import (
	"errors"
	"github.com/cloudfoundry/bosh-azure-storage-cli/client"
	"github.com/cloudfoundry/bosh-azure-storage-cli/client/clientfakes"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"os"
)

var _ = Describe("Client", func() {

	Context("Put", func() {
		It("uploads a file to a blob", func() {
			storageClient := clientfakes.FakeStorageClient{}

			azBlobstore, err := client.New(&storageClient)
			Expect(err).ToNot(HaveOccurred())

			file, _ := os.CreateTemp("", "tmpfile")

			azBlobstore.Put(file.Name(), "target/blob")

			Expect(storageClient.UploadCallCount()).To(Equal(1))
			source, dest := storageClient.UploadArgsForCall(0)

			Expect(source).To(BeAssignableToTypeOf((*os.File)(nil)))
			Expect(dest).To(Equal("target/blob"))
		})

		It("skips the upload if the md5 cannot be calculated from the file", func() {
			storageClient := clientfakes.FakeStorageClient{}

			azBlobstore, err := client.New(&storageClient)
			Expect(err).ToNot(HaveOccurred())

			err = azBlobstore.Put("the/path", "target/blob")

			Expect(storageClient.UploadCallCount()).To(Equal(0))
			Expect(err.Error()).To(Equal("open the/path: no such file or directory"))
		})

		It("fails if the source file md5 does not match the responded md5", func() {
			storageClient := clientfakes.FakeStorageClient{}
			storageClient.UploadReturns([]byte{1, 2, 3}, nil)

			azBlobstore, err := client.New(&storageClient)
			Expect(err).ToNot(HaveOccurred())

			file, _ := os.CreateTemp("", "tmpfile")

			putError := azBlobstore.Put(file.Name(), "target/blob")
			Expect(putError.Error()).To(Equal("the upload responded an MD5 [1 2 3] does not match the source file MD5 [212 29 140 217 143 0 178 4 233 128 9 152 236 248 66 126]"))

			Expect(storageClient.UploadCallCount()).To(Equal(1))
			source, dest := storageClient.UploadArgsForCall(0)
			Expect(source).To(BeAssignableToTypeOf((*os.File)(nil)))
			Expect(dest).To(Equal("target/blob"))

			Expect(storageClient.DeleteCallCount()).To(Equal(1))
			dest = storageClient.DeleteArgsForCall(0)
			Expect(dest).To(Equal("target/blob"))
		})
	})

	It("get blob downloads to a file", func() {
		storageClient := clientfakes.FakeStorageClient{}

		azBlobstore, err := client.New(&storageClient)
		Expect(err).ToNot(HaveOccurred())

		file, _ := os.CreateTemp("", "tmpfile")

		azBlobstore.Get("source/blob", file)

		Expect(storageClient.DownloadCallCount()).To(Equal(1))
		source, dest := storageClient.DownloadArgsForCall(0)

		Expect(source).To(Equal("source/blob"))
		Expect(dest).To(Equal(file))
	})

	It("delete blob deletes the blob", func() {
		storageClient := clientfakes.FakeStorageClient{}

		azBlobstore, err := client.New(&storageClient)
		Expect(err).ToNot(HaveOccurred())

		azBlobstore.Delete("blob")

		Expect(storageClient.DeleteCallCount()).To(Equal(1))
		dest := storageClient.DeleteArgsForCall(0)

		Expect(dest).To(Equal("blob"))
	})

	Context("if the blob existence is checked", func() {
		It("returns blob.Existing on success", func() {
			storageClient := clientfakes.FakeStorageClient{}
			storageClient.ExistsReturns(true, nil)

			azBlobstore, _ := client.New(&storageClient)
			existsState, err := azBlobstore.Exists("blob")
			Expect(existsState == true).To(BeTrue())
			Expect(err).ToNot(HaveOccurred())

			dest := storageClient.ExistsArgsForCall(0)
			Expect(dest).To(Equal("blob"))
		})

		It("returns blob.NotExisting for not existing blobs", func() {
			storageClient := clientfakes.FakeStorageClient{}
			storageClient.ExistsReturns(false, nil)

			azBlobstore, _ := client.New(&storageClient)
			existsState, err := azBlobstore.Exists("blob")
			Expect(existsState == false).To(BeTrue())
			Expect(err).ToNot(HaveOccurred())

			dest := storageClient.ExistsArgsForCall(0)
			Expect(dest).To(Equal("blob"))
		})

		It("returns blob.ExistenceUnknown and an error in case an error occurred", func() {
			storageClient := clientfakes.FakeStorageClient{}
			storageClient.ExistsReturns(false, errors.New("boom"))

			azBlobstore, _ := client.New(&storageClient)
			existsState, err := azBlobstore.Exists("blob")
			Expect(existsState == false).To(BeTrue())
			Expect(err).To(HaveOccurred())

			dest := storageClient.ExistsArgsForCall(0)
			Expect(dest).To(Equal("blob"))
		})
	})

	Context("signed url", func() {
		It("returns a signed url for action 'get'", func() {
			storageClient := clientfakes.FakeStorageClient{}
			storageClient.SignedUrlReturns("https://the-signed-url", nil)

			azBlobstore, _ := client.New(&storageClient)
			url, err := azBlobstore.Sign("blob", "get", 100)
			Expect(url == "https://the-signed-url").To(BeTrue())
			Expect(err).ToNot(HaveOccurred())

			dest, expiration := storageClient.SignedUrlArgsForCall(0)
			Expect(dest).To(Equal("blob"))
			Expect(int(expiration)).To(Equal(100))
		})

		It("fails on unknown action", func() {
			storageClient := clientfakes.FakeStorageClient{}
			storageClient.SignedUrlReturns("", errors.New("boom"))

			azBlobstore, _ := client.New(&storageClient)
			url, err := azBlobstore.Sign("blob", "unknown", 100)
			Expect(url).To(Equal(""))
			Expect(err).To(HaveOccurred())

			Expect(storageClient.SignedUrlCallCount()).To(Equal(0))
		})
	})

})
