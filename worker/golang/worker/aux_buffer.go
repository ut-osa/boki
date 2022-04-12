package worker

import (
	"bytes"
	"encoding/binary"
	"io"
	"log"
)

const kAuxBufferHeaderSize = 16

func encodeAuxBufferHeader(id uint64, size int) []byte {
	buf := make([]byte, kAuxBufferHeaderSize)
	binary.LittleEndian.PutUint64(buf[0:8], id)
	binary.LittleEndian.PutUint64(buf[8:16], uint64(size))
	return buf
}

func decodeAuxBufferHeader(data []byte) ( /* id */ uint64 /* size */, int) {
	id := binary.LittleEndian.Uint64(data[0:8])
	size := binary.LittleEndian.Uint64(data[8:16])
	return id, int(size)
}

func (w *FuncWorker) auxBufferSender() {
	for {
		auxBuf := <-w.auxBufSendChan
		hdr := encodeAuxBufferHeader(auxBuf.id, len(auxBuf.data))
		if _, err := io.Copy(w.engineConn, bytes.NewReader(hdr)); err != nil {
			log.Fatalf("[FATAL] Failed to send aux buffer header: %v", err)
		}
		if _, err := io.Copy(w.engineConn, bytes.NewReader(auxBuf.data)); err != nil {
			log.Fatalf("[FATAL] Failed to send aux buffer: %v", err)
		}
	}
}

func (w *FuncWorker) auxBufferReceiver() {
	for {
		hdr := make([]byte, kAuxBufferHeaderSize)
		if _, err := io.ReadFull(w.engineConn, hdr); err != nil {
			log.Fatalf("[FATAL] Failed to receive aux buffer header: %v", err)
		}
		id, size := decodeAuxBufferHeader(hdr)
		data := make([]byte, size)
		if _, err := io.ReadFull(w.engineConn, data); err != nil {
			log.Fatalf("[FATAL] Failed to receive aux buffer: %v", err)
		}
		// log.Printf("[DEBUG] Receive aux buffer: ID=%#016x, size=%d", id, size)
		auxBuf := &AuxBuffer{id: id, data: data}
		w.auxBufMux.Lock()
		if ch, exists := w.auxBufRecvChans[id]; exists {
			ch <- auxBuf
			delete(w.auxBufRecvChans, id)
		} else {
			newChan := make(chan *AuxBuffer, 1)
			newChan <- auxBuf
			w.auxBufRecvChans[id] = newChan
		}
		w.auxBufMux.Unlock()
	}
}

func (w *FuncWorker) getAuxBufferChan(id uint64) chan *AuxBuffer {
	var ch chan *AuxBuffer
	w.auxBufMux.Lock()
	if c, exists := w.auxBufRecvChans[id]; exists {
		ch = c
		delete(w.auxBufRecvChans, id)
	} else {
		ch = make(chan *AuxBuffer, 1)
		w.auxBufRecvChans[id] = ch
	}
	w.auxBufMux.Unlock()
	return ch
}
