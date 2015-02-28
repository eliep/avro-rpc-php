<?php

const HANDSHAKE_REQUEST_SCHEMA_JSON = <<<HRSJ
{
  "type": "record",
  "name": "HandshakeRequest", "namespace":"org.apache.avro.ipc",
  "fields": [
    {"name": "clientHash",
     "type": {"type": "fixed", "name": "MD5", "size": 16}},
    {"name": "clientProtocol", "type": ["null", "string"]},
    {"name": "serverHash", "type": "MD5"},
    {"name": "meta", "type": ["null", {"type": "map", "values": "bytes"}]}
  ]
}
HRSJ;

const HANDSHAKE_RESPONSE_SCHEMA_JSON = <<<HRSJ
{
  "type": "record",
  "name": "HandshakeResponse", "namespace": "org.apache.avro.ipc",
  "fields": [
    {"name": "match",
     "type": {"type": "enum", "name": "HandshakeMatch",
              "symbols": ["BOTH", "CLIENT", "NONE"]}},
    {"name": "serverProtocol",
     "type": ["null", "string"]},
    {"name": "serverHash",
     "type": ["null", {"type": "fixed", "name": "MD5", "size": 16}]},
    {"name": "meta",
     "type": ["null", {"type": "map", "values": "bytes"}]}
  ]
}
HRSJ;

const BUFFER_SIZE = 8192;


/**
 * Exceptions
 */

/**
 * Raised when an error message is sent by an Avro requestor or responder.
 * @package Avro
 */
class AvroRemoteException extends AvroException { }

/**
 * @package Avro
 */
class ConnectionClosedException extends AvroException { }



/**
 * Base IPC Classes (Requestor/Responder)
 */

/**
 * Base class for the client side of a protocol interaction.
 * @package Avro
 */
class Requestor {
  
  protected $local_protocol;
  protected $transceiver;
  protected $remote_protocol = null;
  protected $remote_hash = null;
  protected $send_protocol = false;
  
  protected $handshake_requestor_writer;
  protected $handshake_requestor_reader;
  protected $meta_writer;
  protected $meta_reader;
  
  /**
   * Initializes a new requestor object
   * @param AvroProtocol $local_protocol Avro Protocol describing the messages sent and received.
   * @param Transceiver $transceiver Transceiver instance to channel messages through.
   */
  public function __construct(AvroProtocol $local_protocol, Transceiver $transceiver) {
    $this->local_protocol = $local_protocol;
    $this->transceiver = $transceiver;
    $this->handshake_requestor_writer = new AvroIODatumWriter(AvroSchema::parse(HANDSHAKE_REQUEST_SCHEMA_JSON));
    $this->handshake_requestor_reader = new AvroIODatumReader(AvroSchema::parse(HANDSHAKE_RESPONSE_SCHEMA_JSON));
    $this->meta_writer = new AvroIODatumWriter(AvroSchema::parse('{"type": "map", "values": "bytes"}'));
    $this->meta_reader = new AvroIODatumReader(AvroSchema::parse('{"type": "map", "values": "bytes"}'));
  }
  
  public function local_protocol() {
    return $this->local_protocol;
  }
  
  public function transceiver() {
    return $this->trancseiver;
  }
  
  
  /**
   * Writes a request message and reads a response or error message.
   * @param string $message_name : name of the IPC method
   * @param mixed $request_datum : IPC request
   * @throw AvroException when $message_name is not registered on the local or remote protocol
   * @throw AvroRemoteException when server send an error
   */
  public function request($message_name, $request_datum) {
    $io = new AvroStringIO();
    $encoder = new AvroIOBinaryEncoder($io);
    $this->write_handshake_request($encoder);
    $this->write_call_request($message_name, $request_datum, $encoder);
    
    $call_request = $io->string();
    $call_response = $this->transceiver->transceive($call_request);
    
    // process the handshake and call response
    $io = new AvroStringIO($call_response);
    $decoder = new AvroIOBinaryDecoder($io);
    $call_response_exists = $this->read_handshake_response($decoder);
    
    if ($call_response_exists)
      return $this->read_call_response($message_name, $decoder);
    else
      return $this->request($message_name, $request_datum);
  }
  
  /**
   * Write the handshake request.
   * @param AvroIOBinaryEncoder $encoder : Encoder to write the handshake request into.
   */
  public function write_handshake_request(AvroIOBinaryEncoder $encoder) {
    $local_hash = $this->local_protocol->md5();
    if (is_null($this->remote_hash)) {
      $this->remote_hash = $local_hash;
      $this->remote_protocol = $this->local_protocol;
    }
    
    $request_datum = array('clientHash' => $local_hash, 'serverHash' => $this->remote_hash, 'meta' => null);
    $request_datum["clientProtocol"] = ($this->send_protocol) ? $this->local_protocol : null;
      
    $this->handshake_requestor_writer->write($request_datum, $encoder);
  }
  
  /**
   * The format of a call request is:
   * - request metadata, a map with values of type bytes
   * - the message name, an Avro string, followed by
   * - the message parameters. Parameters are serialized according to the message's request declaration.
   * @param string $message_name : name of the IPC method
   * @param mixed $request_datum : IPC request
   * @param AvroIOBinaryEncoder $encoder : Encoder to write the handshake request into.
   * @throw AvroException when $message_name is not registered on the local protocol
   */
  public function write_call_request($message_name, $request_datum, AvroIOBinaryEncoder $encoder) {
    $request_metadata = array();
    $this->meta_writer->write($request_metadata, $encoder);
    
    if (!isset($this->local_protocol->messages[$message_name]))
      throw new AvroException("Unknown message: $message_name");
    
    $encoder->write_string($message_name);
    $writer = new AvroIODatumWriter($this->local_protocol->messages[$message_name]->request);
    $writer->write($request_datum, $encoder);
  }
  
  /**
   * Reads and processes the handshake response message.
   * @param AvroIOBinaryDecoder $decoder : Decoder to read messages from.
   * @return boolean true if a response exists.
   * @throw  AvroException when server respond an unknown handshake match 
   */
  public function read_handshake_response(AvroIOBinaryDecoder $decoder) {
    $handshake_response = $this->handshake_requestor_reader->read($decoder);
    $match = $handshake_response["match"];
    switch ($match) {
      case 'BOTH':
        $this->send_protocol = false;
        return true;
        break;
      case 'CLIENT':
        $this->remote_protocol = AvroProtocol::parse($handshake_response["serverProtocol"]);
        $this->remote_hash = $handshake_response["serverHash"];
        $this->send_protocol = false;
        return true;
        break;
      case 'NONE':
        $this->remote_protocol = AvroProtocol::parse($handshake_response["serverProtocol"]);
        $this->remote_hash = $handshake_response["serverHash"];
        $this->send_protocol = false;
        return false;
        break;
      default:
        throw new AvroException("Bad handshake response match: $match");
    }
  }

  
  /**
   * Reads and processes a method call response.
   * The format of a call response is:
   * - response metadata, a map with values of type bytes
   * - a one-byte error flag boolean, followed by either:
   * - if the error flag is false, the message response, serialized per the message's response schema.
   * - if the error flag is true, the error, serialized per the message's error union schema.
   * @param string $message_name : name of the IPC method
   * @param AvroIOBinaryDecoder $decoder : Decoder to read messages from.
   * @return boolean true if a response exists.
   * @throw  AvroException $message_name is not registered on the local or remote protocol
   * @throw AvroRemoteException when server send an error
   */
  public function read_call_response($message_name, AvroIOBinaryDecoder  $decoder) {
    $response_metadata = $this->meta_reader->read($decoder);
    
    if (!isset($this->remote_protocol->messages[$message_name]))
      throw new AvroException("Unknown remote message: $message_name");
    $remote_message_schema = $this->remote_protocol->messages[$message_name];
    
    if (!isset($this->local_protocol->messages[$message_name]))
      throw new AvroException("Unknown local message: $message_name");
    $local_message_schema = $this->local_protocol->messages[$message_name];
    
    // No error raised on the server
    if (!$decoder->read_boolean()) {
      $datum_reader = new AvroIODatumReader($remote_message_schema->response, $local_message_schema->response);
      return $datum_reader->read($decoder);
    } else {
      $datum_reader = new AvroIODatumReader($remote_message_schema->errors, $local_message_schema->errors);
      throw new AvroRemoteException($datum_reader->read($decoder));
    }
  }
  
}

/**
 * Base class for the server side of a protocol interaction.
 */
class Responder {
  
  protected $local_protocol;
  protected $local_hash;
  protected $protocol_cache = array();
  
  protected $handshake_responder_writer;
  protected $handshake_responder_reader;
  protected $meta_writer;
  protected $meta_reader;

  protected $system_error_schema;
  
  public function __construct(AvroProtocol $local_protocol) {
    $this->local_protocol = $local_protocol;
    $this->local_hash = $local_protocol->md5();
    $this->protocol_cache[$this->local_hash] = $this->local_protocol;
  
    $this->handshake_responder_writer = new AvroIODatumWriter(AvroSchema::parse(HANDSHAKE_RESPONSE_SCHEMA_JSON));
    $this->handshake_responder_reader = new AvroIODatumReader(AvroSchema::parse(HANDSHAKE_REQUEST_SCHEMA_JSON));
    $this->meta_writer = new AvroIODatumWriter(AvroSchema::parse('{"type": "map", "values": "bytes"}'));
    $this->meta_reader = new AvroIODatumReader(AvroSchema::parse('{"type": "map", "values": "bytes"}'));

    $this->system_error_schema = AvroSchema::parse('["string"]');
  }
  
  /**
   * @param string $hash hash of an Avro Protocol
   * @return AvroProtocol|null The protocol associated with $hash or null
   */
  public function get_protocol_cache($hash) {
    return (isset($this->protocol_cache[$hash])) ? $this->protocol_cache[$hash] : null;
  }
  
  /**
   * @param string $hash hash of an Avro Protocol
   * @param AvroProtocol $protocol
   * @return Responder $this
   */
  public function set_protocol_cache($hash, AvroProtocol $protocol) {
    $this->protocol_cache[$hash] = $protocol;
    return $this;
  }
  
  public function local_protocol() {
    return $this->local_protocol;
  }
  
  /**
   * Entry point to process one procedure call.
   * @param string $call_request the serialized procedure call request
   * @return string the serialiazed procedure call response
   * @throw AvroException
   */
  public function respond($call_request) {
    $buffer_reader = new AvroStringIO($call_request);
    $decoder = new AvroIOBinaryDecoder($buffer_reader);
    
    $buffer_writer = new AvroStringIO();
    $encoder = new AvroIOBinaryEncoder($buffer_writer);
    
    $error = null;
    $response_metadata = array();
    try {
      $remote_protocol = $this->process_handshake($decoder, $encoder);
      if (is_null($remote_protocol))
        return $buffer_writer->string();
      
      $request_metadata = $this->meta_reader->read($decoder);
      $remote_message_name = $decoder->read_string();
      if (!isset($remote_protocol->messages[$remote_message_name]))
        throw new AvroException("Unknown remote message: $remote_message_name");
      $remote_message = $remote_protocol->messages[$remote_message_name];
      
      if (!isset($this->local_protocol->messages[$remote_message_name]))
        throw new AvroException("Unknown local message: $remote_message_name");
      $local_message = $this->local_protocol->messages[$remote_message_name];
      
      $datum_reader = new AvroIODatumReader($remote_message->request, $local_message->request);
      $request = $datum_reader->read($decoder);
      try {
        $response_datum = $this->invoke($local_message, $request);
      } catch (AvroRemoteException $e) {
        $error = $e;
      } catch (Exception $e) {
        $error = new AvroRemoteException($e->getMessage());
      }
      $this->meta_writer->write($response_metadata, $encoder);
      $encoder->write_boolean(!is_null($error));
      
      if (is_null($error)) {
        $datum_writer = new AvroIODatumWriter($local_message->response);
        $datum_writer->write($response_datum, $encoder);
      } else {
        $datum_writer = new AvroIODatumWriter($local_message->errors);
        $datum_writer->write($error->getMessage(), $encoder);
      }
    } catch (AvroException $e) {
      $error = new AvroRemoteException($e->getMessage());
      $buffer_writer = new AvroStringIO();
      $encoder = new AvroIOBinaryEncoder($buffer_writer);
      $this->meta_writer->write($response_metadata, $encoder);
      $encoder->write_boolean(!is_null($error));
      $datum_writer = new AvroIODatumWriter($this->system_error_schema);
      $datum_writer->write($error->getMessage(), $encoder);
    }
    
    return $buffer_writer->string();
  }

  /**
   * Processes an RPC handshake.
   * @param AvroIOBinaryDecoder $decoder Where to read from
   * @param AvroIOBinaryEncoder $encoder  Where to write to.
   * @return AvroProtocol The requested Protocol.
   */
  public function process_handshake(AvroIOBinaryDecoder $decoder, AvroIOBinaryEncoder $encoder) {
    $handshake_request = $this->handshake_responder_reader->read($decoder);
    $client_hash = $handshake_request["clientHash"];
    $client_protocol = $handshake_request["clientProtocol"];
    $remote_protocol =  $this->get_protocol_cache($client_hash);
    
    if (is_null($remote_protocol) && !is_null($client_protocol)) {
      $remote_protocol = Protocol::parse($client_protocol);
      $this->set_protocol_cache($client_hash, $remote_protocol);
    }
    
    $server_hash = $handshake_request["serverHash"];
    $handshake_response = array();
    
    if ($this->local_hash == $server_hash)
      $handshake_response['match'] = (is_null($remote_protocol)) ? 'NONE' : 'BOTH';
    else
      $handshake_response['match'] = (is_null($remote_protocol)) ? 'NONE' : 'CLIENT';

    $handshake_response["meta"] = null;
    if ($handshake_response['match'] != 'BOTH') {
      $handshake_response["serverProtocol"] = $this->local_protocol->__toString();
      $handshake_response["serverHash"] = $this->local_hash;
    } else {
      $handshake_response["serverProtocol"] = null;
      $handshake_response["serverHash"] = null;
    }
    
    $this->handshake_responder_writer->write($handshake_response, $encoder);
    
    return $remote_protocol;
  }
  
  /**
   * Processes one procedure call
   * @param AvroProtocolMessage $local_message
   * @param mixed $request Call request
   * @return mixed Call response
   */
  public function invoke( $local_message, $request) {
  
  }
}

/**
 * Abstract class to handle communicaton (framed read & write) between client & server
 */
abstract class Transceiver {
  public $remote_name;
  
  /**
   * Processes a single request-reply interaction. Synchronous request-reply interaction.
   * @param string $request the request message
   * @return string the reply message
   */
  public function transceive($request) {
    $this->write_message($request);
    return $this->read_message();
  }
  
  /**
   * Reads a single message from the channel.
   * Blocks until a message can be read.
   * @return string The message read from the channel.
   */
  abstract public function read_message();
  
  /**
   * Writes a message into the channel. Blocks until the message has been written.
   * @param string $message
   */
  abstract public function write_message($message);
  
  /**
   * Close this transceiver
   */
  abstract public function close();
}

/**
 * Socket Transceiver implementation.
 * This class can be used by a client to communicate with a server
 */
class SocketTransceiver extends Transceiver {
  
  private static $serial = 1;
  protected $socket;
  
  public function __construct($host, $port) {
    $this->socket = socket_create(AF_INET, SOCK_STREAM, 0);
    socket_connect($this->socket , $host , $port);
  }
  
  public function read_message() {
    socket_recv ( $this->socket , $buf , 8 , MSG_WAITALL );
    if ($buf == null)
      return $buf;
    
    $frame_count = unpack("Nserial/Ncount", $buf);
    $frame_count = $frame_count["count"];
    $message = "";
    for ($i = 0; $i < $frame_count; $i++ ) {
      socket_recv ( $this->socket , $buf , 4 , MSG_WAITALL );
      $frame_size = unpack("Nsize", $buf);
      $frame_size = $frame_size["size"];
      socket_recv ( $this->socket , $buf , $frame_size , MSG_WAITALL );
      $message .= $buf;
    }
    
    return $message;
  }
  
  public function write_message($message) {
    $binary_length = strlen($message);
    
    $max_binary_frame_length = BUFFER_SIZE - 4;
    $sended_length = 0;
    
    $frames = array();
    while ($sended_length < $binary_length) {
      $not_sended_length = $binary_length - $sended_length;
      $binary_frame_length = ($not_sended_length > $max_binary_frame_length) ? $max_binary_frame_length : $not_sended_length;
      $frames[] = substr($message, $sended_length, $binary_frame_length);
      $sended_length += $binary_frame_length;
    }
    
    $header = pack("N", self::$serial++).pack("N", count($frames));
    //socket_send ($this->socket, $header, strlen($header) , 0);
    $x = socket_write ($this->socket, $header, strlen($header));
    foreach ($frames as $frame) {
      $msg = pack("N", strlen($frame)).$frame;
      socket_write ( $this->socket, $msg, strlen($msg));
    }
    
  }
  
  public function close() {
    socket_close($this->socket);
  }
  
}

/**
 * Socket server implementation.
 */
class SocketServer extends SocketTransceiver {
  
  protected $responder;
  protected $socket0;
  
  public function __construct($host, $port, $responder) {
    $this->responder = $responder;
    $this->socket0 = socket_create(AF_INET, SOCK_STREAM, 0);
    socket_bind($this->socket0 , $host , $port);
    socket_listen($this->socket0, 3);
  }
  
  public function start($max_clients = 10) {
    $clients = array();
    
    while (true) {
      // $read contains all the client we listen to
      $read = array($this->socket0);
      for ($i = 0; $i < $max_clients; $i++) {
        if (isset($clients[$i]) && $clients[$i]  != null)
          $read[$i + 1] = $clients[$i];
      }
    
      // check all client to know which ones are writing
      $ready = socket_select($read, $write , $except, null);
      // $read contains all client that send something to the server
      
      // New connexion
      if (in_array($this->socket0, $read)) {
        for ($i = 0; $i < $max_clients; $i++) {
          if ( !isset($clients[$i]) ) {
            $clients[$i] = socket_accept($this->socket0);
            break;
          }
        }
      }
      
      // Check all client that are trying to write
      for ($i = 0; $i < $max_clients; $i++) {
        if (isset($clients[$i]) && in_array($clients[$i] , $read)) {
          $this->socket = $clients[$i];
          // Read the message
          $call_request = $this->read_message();
          // Respond if the message is not empty
          if (!is_null($call_request)) {
            $call_response = $this->responder->respond($call_request);
            $this->write_message($call_response);
          // Else it's a client disconnecton
          } else {
            socket_close($clients[$i]);
            unset($clients[$i]);
          }
        }
      }
    } 
  }
}
  


/**
 * Framed message
 */

/**
 * Wrapper around a file-like object to read framed data.
 */
class FramedReader {
  
  protected $reader;
  
  public function __construct($reader) {
    $this->reader = $reader;
  }
  
  /**
   * Reads one message from the configured reader.
   * @return string the message
   */
  public function read() {
    
  }
}