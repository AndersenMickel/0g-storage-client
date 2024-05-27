package transfer

import (
	"fmt"

	"github.com/0glabs/0g-storage-client/common/parallel"
	"github.com/0glabs/0g-storage-client/core"
	"github.com/0glabs/0g-storage-client/node"
	"github.com/0glabs/0g-storage-client/transfer/download"
	"github.com/ethereum/go-ethereum/common"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const minBufferSize = 8

type SegmentDownloader struct {
	clients []*node.Client
	file    *download.DownloadingFile

	withProof bool

	segmentOffset uint64
	numChunks     uint64
	numSegments   uint64
}

var _ parallel.Interface = (*SegmentDownloader)(nil)

func NewSegmentDownloader(clients []*node.Client, file *download.DownloadingFile, withProof bool) (*SegmentDownloader, error) {
	offset := file.Metadata().Offset
	if offset%core.DefaultSegmentSize > 0 {
		return nil, errors.Errorf("invalid data offset in downloading file: %v", offset)
	}

	fileSize := file.Metadata().Size

	return &SegmentDownloader{
		clients: clients,
		file:    file,

		withProof:     withProof,
		segmentOffset: uint64(offset / core.DefaultSegmentSize),
		numChunks:     core.NumSplits(fileSize, core.DefaultChunkSize),
		numSegments:   core.NumSplits(fileSize, core.DefaultSegmentSize),
	}, nil
}

// Download downloads segments in parallel.
func (downloader *SegmentDownloader) Download() error {
	numTasks := downloader.numSegments - downloader.segmentOffset
	numNodes := len(downloader.clients)
	bufSize := numNodes * 2
	if bufSize < minBufferSize {
		bufSize = minBufferSize
	}

	return parallel.Serial(downloader, int(numTasks), numNodes, bufSize)
}

// ParallelDo implements the parallel.Interface interface.
func (downloader *SegmentDownloader) ParallelDo(routine, task int) (interface{}, error) {
	segmentIndex := downloader.segmentOffset + uint64(task)
	startIndex := segmentIndex * core.DefaultSegmentMaxChunks
	endIndex := startIndex + core.DefaultSegmentMaxChunks
	if endIndex > downloader.numChunks {
		endIndex = downloader.numChunks
	}

	root := downloader.file.Metadata().Root
	var segment []byte
	var err error

	if downloader.withProof {
		segment, err = downloader.downloadSegmentWithProof(downloader.clients[routine], root, startIndex, endIndex)
	} else {
		segment, err = downloader.clients[routine].ZeroGStorage().DownloadSegment(root, startIndex, endIndex)
	}

	if err != nil {
		logrus.WithError(err).WithFields(logrus.Fields{
			"routine": routine,
			"segment": fmt.Sprintf("%v/%v", segmentIndex, downloader.numSegments),
			"chunks":  fmt.Sprintf("[%v, %v)", startIndex, endIndex),
		}).Error("failed to download segment")
	} else if logrus.IsLevelEnabled(logrus.TraceLevel) {
		logrus.WithFields(logrus.Fields{
			"routine": routine,
			"segment": fmt.Sprintf("%v/%v", segmentIndex, downloader.numSegments),
			"chunks":  fmt.Sprintf("[%v, %v)", startIndex, endIndex),
		}).Trace("succeeded to download segment")
	}

	// Remove paddings for the last chunk
	if err == nil && segmentIndex == downloader.numSegments-1 {
		fileSize := downloader.file.Metadata().Size
		if lastChunkSize := fileSize % core.DefaultChunkSize; lastChunkSize > 0 {
			paddings := core.DefaultChunkSize - lastChunkSize
			segment = segment[0 : len(segment)-int(paddings)]
		}
	}

	return segment, err
}

// ParallelCollect implements the parallel.Interface interface.
func (downloader *SegmentDownloader) ParallelCollect(result *parallel.Result) error {
	return downloader.file.Write(result.Value.([]byte))
}

func (downloader *SegmentDownloader) downloadSegmentWithProof(client *node.Client, root common.Hash, startIndex, endIndex uint64) ([]byte, error) {
	segmentIndex := startIndex / core.DefaultSegmentMaxChunks

	segment, err := client.ZeroGStorage().DownloadSegmentWithProof(root, segmentIndex)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to download segment with proof from storage node")
	}

	if expectedDataLen := (endIndex - startIndex) * core.DefaultChunkSize; int(expectedDataLen) != len(segment.Data) {
		return nil, errors.Errorf("downloaded data length mismatch, expected = %v, actual = %v", expectedDataLen, len(segment.Data))
	}

	numChunksFlowPadded, _ := core.ComputePaddedSize(downloader.numChunks)
	numSegmentsFlowPadded := (numChunksFlowPadded-1)/core.DefaultSegmentMaxChunks + 1

	// Pad empty chunks for the last segment to validate Merkle proof
	var emptyChunksPadded uint64
	if numChunks := endIndex - startIndex; numChunks < core.DefaultSegmentMaxChunks {
		if segmentIndex < numSegmentsFlowPadded-1 || numChunksFlowPadded%core.DefaultSegmentMaxChunks == 0 {
			// Pad empty chunks to a full segment
			emptyChunksPadded = core.DefaultSegmentMaxChunks - numChunks
		} else if lastSegmentChunks := numChunksFlowPadded % core.DefaultSegmentMaxChunks; numChunks < lastSegmentChunks {
			// Pad for the last segment with flow padded empty chunks
			emptyChunksPadded = lastSegmentChunks - numChunks
		}
	}

	segmentRootHash := core.SegmentRoot(segment.Data, emptyChunksPadded)

	if err := segment.Proof.ValidateHash(root, segmentRootHash, segmentIndex, numSegmentsFlowPadded); err != nil {
		return nil, errors.WithMessage(err, "failed to validate proof")
	}

	return segment.Data, nil
}
