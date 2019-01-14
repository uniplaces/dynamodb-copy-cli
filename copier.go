package dynamodbcopy

import "sync"

type Copier interface {
	Copy() error
}

type copyService struct {
	srcTable     DynamoDBService
	trgTable     DynamoDBService
	totalReaders int
	totalWriters int
}

func NewCopier(srcTableService, trgTableService DynamoDBService, totalReaders, totalWriters int) Copier {
	return copyService{
		srcTable:     srcTableService,
		trgTable:     trgTableService,
		totalReaders: totalReaders,
		totalWriters: totalWriters,
	}
}

func (copyService copyService) Copy() error {
	errChan := make(chan error)
	items := make(chan []DynamoDBItem)

	wgReaders := &sync.WaitGroup{}
	wgReaders.Add(copyService.totalReaders)

	wgWriters := &sync.WaitGroup{}
	wgWriters.Add(copyService.totalWriters)

	for i := 0; i < copyService.totalReaders; i++ {
		go copyService.read(i, wgReaders, items, errChan)
	}

	for i := 0; i < copyService.totalWriters; i++ {
		go copyService.write(wgWriters, items, errChan)
	}

	go func() {
		wgReaders.Wait()
		close(items)
		wgWriters.Wait()
		close(errChan)
	}()

	return <-errChan
}

func (copyService copyService) read(id int, wg *sync.WaitGroup, itemsChan chan []DynamoDBItem, errChan chan error) {
	defer wg.Done()

	if err := copyService.srcTable.Scan(itemsChan, copyService.totalReaders, id); err != nil {
		errChan <- err
	}
}

func (copyService copyService) write(wg *sync.WaitGroup, itemsChan chan []DynamoDBItem, errChan chan error) {
	defer wg.Done()

	if err := copyService.trgTable.BatchWrite(<-itemsChan); err != nil {
		errChan <- err
	}
}
