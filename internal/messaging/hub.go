package messaging

import (
	"log"
	"sync"
)

type ChanPair struct{
	MsgChan chan *Msg
	ErrChan chan *Msg
}

type Hub struct {
	sync.RWMutex
	clients map[string] ChanPair //AcctID : ChanPair
}

var WSHub = &Hub{
	clients: make(map[string] ChanPair),
}

func (h *Hub) Register(acctID string, msgChan, errChan chan *Msg){
	h.Lock()
	defer h.Unlock()
	h.clients[acctID] = ChanPair{
		MsgChan: msgChan,
		ErrChan: errChan,
	}
}

func (h *Hub) Unregister(acctID string){
	h.Lock()
	defer h.Unlock()
	delete(h.clients, acctID)
}

func (h *Hub) PushMsgChan(acctID string, msg *Msg) bool{
	const handlerName= "push-msg-to-channel-handler"

	h.RLock()
	chpair, exists:= h.clients[acctID]
	h.RUnlock()

	if!exists{
		log.Printf("[%s] Account %s disconnected from this instance", handlerName, acctID)
		return false
	}

	select{
	case chpair.MsgChan <- msg:
		return true
	default:
		log.Printf("[%s] Recieving Msg channel is full", handlerName)
		return false
	}
}

func (h *Hub) PushErrChan(acctID string, err *Msg) bool{
	const handlerName= "push-err-to-channel-handler"

	h.RLock()
	chpair, exists:= h.clients[acctID]
	h.RUnlock()

	if!exists{
		log.Printf("[%s] Err channel does not exist for this account %s", handlerName, acctID)
		return false
	}

	select{
	case chpair.ErrChan <- err:
		return true
	default:
		log.Printf("[%s] Recieving Err channel is full", handlerName)
		return false
	}
}