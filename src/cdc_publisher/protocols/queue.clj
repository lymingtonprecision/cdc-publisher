(ns cdc-publisher.protocols.queue)

(defprotocol QueueReader
  (dequeue! [this queue]
    "Dequeues and returns the next message from `queue`, immediately
    marking it as read. Returns `nil` if no message is available or an
    invalid queue is specified.")
  (dequeue-sync [this queue f]
    "Dequeues the next message from `queue` passing it to `f` and only
    committing it as read from the queue if `f` returns without
    raising an exception.

    Returns the result of `(f msg)`.

    If no message is available or an invalid queue is specified then
    `f` will _not_ be called and `nil` returned."))

(defprotocol QueueWriter
  (enqueue! [this queue msg]))
