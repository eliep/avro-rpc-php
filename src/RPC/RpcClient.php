<?php

namespace Avro\RPC;


class RpcClient extends RpcTransport {
  
  private $frame_length = 1024;
  private static $serial = 0;
  
  public function __construct($host, $port) {
    parent::__construct($host, $port);
    $this->socket = socket_create(AF_INET, SOCK_STREAM, 0);
    socket_connect($this->socket , $host , $port);
  }
  
  public function encodeRequest($protocolWrapper, $method, $params) {
    $io = new \AvroStringIO();
    $encoder = new \AvroIOBinaryEncoder($io);
    if (!$this->handshake) {
      $protocolWrapper->writeRequestHandshake($encoder);
      $this->handshake = true;
    }
    $protocolWrapper->writeMetadata($encoder);
    $encoder->write_string($method);
    $protocolWrapper->writeRequest($encoder, $method, $params);
    
    return $io->string();
  }
  
  
  public function send($protocolWrapper, $method, $params) {
    $binary = $this->encodeRequest($protocolWrapper, $method, $params);
    $binary_length = strlen($binary);
    
    $max_binary_frame_length = $this->frame_length - 4;
    $sended_length = 0;
    
    $frames = array();
    while ($sended_length < $binary_length) {
      $not_sended_length = $binary_length - $sended_length;
      $binary_frame_length = ($not_sended_length > $max_binary_frame_length) ? $max_binary_frame_length : $not_sended_length;
      $frames[] = substr($binary, $sended_length, $binary_frame_length);
      $sended_length += $binary_frame_length;
    }
    
    $header = pack("N", self::$serial++).pack("N", count($frames));
    //socket_send ($this->socket, $header, strlen($header) , 0);
    $x = socket_write ($this->socket, $header, strlen($header));
    foreach ($frames as $frame) {
      $msg = pack("N", strlen($frame)).$frame;
      //socket_send ( $this->socket, $msg, strlen($msg), 0);
      $x = socket_write ( $this->socket, $msg, strlen($msg));
    }
  }
  
  public function decodeResponse($protocolWrapper, $method, $binary) {
    $io = new \AvroStringIO($binary);
    $decoder = new \AvroIOBinaryDecoder($io);
    $protocolWrapper->readResponseHandshake($decoder);
    $protocolWrapper->readMetadata($decoder);
    $error_flag = $decoder->read_boolean();
    
    if (!$error_flag) {
      return $protocolWrapper->readResponse($decoder, $method);
    } else {
      $error = $protocolWrapper->readError($decoder);
      throw new RpcResponseException($error);
    }
  }
  
  public function read($protocolWrapper, $method) {
    socket_recv ( $this->socket , $buf , 8 , MSG_WAITALL );
    $frame_count = unpack("Nserial/Ncount", $buf);
    $frame_count = $frame_count["count"];
    $binary = "";
    for ($i = 0; $i < $frame_count; $i++ ) {
      socket_recv ( $this->socket , $buf , 4 , MSG_WAITALL );
      $frame_size = unpack("Nsize", $buf);
      $frame_size = $frame_size["size"];
      socket_recv ( $this->socket , $buf , $frame_size , MSG_WAITALL );
      $binary .= $buf;
    }
    
    return $this->decodeResponse($protocolWrapper, $method, $binary);
  }
}
