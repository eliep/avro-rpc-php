<?php

namespace Avro\RPC;


class RpcServer {
  
  private $socket;
  private $spawn;
  private $frame_length = 1024;
  
  public function __construct($host, $port) {
    $this->socket = socket_create(AF_INET, SOCK_STREAM, 0);
    socket_bind($this->socket , $host , $port);
    socket_listen($this->socket, 3);
    $this->spawn = socket_accept($this->socket);
  }
  
  public function encodeResult($protocolWrapper, $method, $result) {
    $io = new \AvroStringIO();
    $encoder = new \AvroIOBinaryEncoder($io);
    $protocolWrapper->writeResponseHandshake($encoder);
    $protocolWrapper->writeMetadata($encoder);
    $encoder->write_boolean(false);
    $protocolWrapper->writeResponse($encoder, $method, $result);
    
    return $io->string();
  }
  
  
  public function send($protocolWrapper, $method, $result) {
    $binary = $this->encodeResult($protocolWrapper, $method, $result);
    
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
    
    $header = pack("N", 1).pack("N", count($frames));
    socket_send ($this->spawn, $header, strlen($header) , 0);
    foreach ($frames as $frame) {
      $msg = pack("N", strlen($frame)).$frame;
      socket_send ( $this->spawn, $msg, strlen($msg), 0);
    }
    $footer = pack("N", 0);
    socket_send ( $this->spawn, $footer, strlen($footer), 0);
  }
  
  public function decodeRequest($protocolWrapper, $binary, &$method) {
    $io = new \AvroStringIO($binary);
    $decoder = new \AvroIOBinaryDecoder($io);
    $protocolWrapper->readRequestHandshake($decoder);
    $protocolWrapper->readMetadata($decoder);
    $method = $decoder->read_string();
    
    return $protocolWrapper->readRequest($decoder, $method);
  }
  
  public function read($protocolWrapper, &$method) {
    socket_recv ( $this->spawn , $buf , 8 , MSG_WAITALL );
    $frame_count = unpack("Nserial/Ncount", $buf);
    $frame_count = $frame_count["count"];
    $binary = "";
    for ($i = 0; $i < $frame_count; $i++ ) {
      socket_recv ( $this->spawn , $buf , 4 , MSG_WAITALL );
      $frame_size = unpack("Nsize", $buf);
      $frame_size = $frame_size["size"];
      socket_recv ( $this->spawn , $buf , $frame_size , MSG_WAITALL );
      $binary .= $buf;
    }
    socket_recv ( $this->spawn , $buf , 4 , MSG_WAITALL );
    
    return $this->decodeRequest($protocolWrapper, $binary, $method);
    
  }
}
