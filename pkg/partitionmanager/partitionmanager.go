package partitionmanager

import (
	"context"
	"hash/fnv"
	"io"

	"github.com/wwq1988/idgenerator/pkg/idgetter"
	"github.com/wwq1988/idgenerator/pkg/partition"
)

const (
	partitionNumber = 1024
)

type partitionManager struct {
	partitions [partitionNumber]idgetter.IDGetter
}

// New New
func New(partitionFactory partition.Factory) idgetter.IDGetter {
	var partitions [partitionNumber]idgetter.IDGetter
	for i := 0; i < partitionNumber; i++ {
		partitions[i] = partitionFactory()
	}
	m := &partitionManager{
		partitions: partitions,
	}
	return m
}

func (m *partitionManager) GetID(ctx context.Context, biz string) (int64, error) {
	partition := m.getPartition(biz)
	return partition.GetID(ctx, biz)

}

func (m *partitionManager) getPartition(biz string) idgetter.IDGetter {
	h := fnv.New32()
	io.WriteString(h, biz)
	pos := h.Sum32() % partitionNumber
	return m.partitions[pos]
}
