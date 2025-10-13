package consumer

type Handler func(msg []byte) error
