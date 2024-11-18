ListenTCPRecordWrite

Main function: Listen to TCP Port - Write Records, marked and batched by Sender
Additional function: Send back Packets to Sender, if FlowFile is marked

Focus: Non-Blocking receive, internal buffering in Queues, Blocking RecordWriters w/ TimeOuts

Not treated: Encoding of Response

Original SocketChannelRecordReader -> became BufferedChannelRecordReader
* Hold RecordReader (stays the same)
* Expose a receiverOutputStream() for the internal Queue
* Add inputStream.available() around the internal Queue to make ASN.1 reader pluggable to Network Sockets
* Not directly addressable. When processor is triggered without an input FF, next ChannelRecordReader is polled from a queue

Original SocketChannelRecordReaderDispatcher -> new Functionalities
* Opens the SocketChannel directly via java.nio
* Reads ATTACH Selectors.keys , creates a BufferedChannelRecordReader per Sender (similar)
* Adds the BufferedChannelRecordReader as attachment to the Selector.Key
* Reads DATA Selector.Keys and delivers raw Data to the recordReader-queue

