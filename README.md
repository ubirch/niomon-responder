# Niomon Responder
The responsibility of this microservice is to take a message from niomon and prepare it to be sent back to the user.
It creates a UPP with a configured device id, fills it with a per-device message, and sends it to the signer.

## Development
Practically all the code is contained in [MessageDecoderMicroservice](./src/main/scala/com/ubirch/responder/ResponderMicroservice.scala).
