(ns cdc-publisher.protocols.ccd-store)

(defprotocol CCDStore
  (known-ccds [this]
    "Returns a tuple of `[ccds ts]` where `ccds` is a collection of
    all known CCDs, in their last known state, and `ts` is a store
    specific token that can be used to refer to this point in the CCD
    stores history.")
  (post-updates-to-chan [this ch] [this ch as-of]
    "Creates and returns a `java.io.Closeable` that encapsulates a
    non-blocking process which puts updates to CCDs generated after
    `as-of` (or the time of the call if `as-of` is not provided) onto
    the supplied channel as and when they occur (immediately sending
    all updates between `as-of` and now to the channel if it is in the
    past.)

    Calling `.close` on the returned object should immediately cease
    publication to the channel."))
