<?php

namespace Avro\RPC;


class RpcServer  extends RpcTransport {
  
  private $spawn;
  
  public function __construct($host, $port) {
    parent::__construct($host, $port);
    $this->socket = socket_create(AF_INET, SOCK_STREAM, 0);
    socket_bind($this->socket , $host , $port);
    socket_listen($this->socket, 3);
  }
  
  public function send($protocolWrapper, $method, $result) {
    $binary = $this->encode($protocolWrapper, $method, $result);
    $this->write($this->spawn, $binary);
  }
  
  public function receive($protocolWrapper, &$method) {
    $this->spawn = socket_accept($this->socket);
    $binary = $this->read($this->spawn);
    return $this->decode($protocolWrapper, $binary, $method);
  }
  
  public function encode($protocolWrapper, $method, $result) {
    $io = new \AvroStringIO();
    $encoder = new \AvroIOBinaryEncoder($io);
    $protocolWrapper->writeResponseHandshake($encoder);
    $protocolWrapper->writeMetadata($encoder);
    $encoder->write_boolean(false);
    $protocolWrapper->writeResponse($encoder, $method, $result);
    
    return $io->string();
  }
  
  public function decode($protocolWrapper, $binary, &$method) {
    $io = new \AvroStringIO($binary);
    $decoder = new \AvroIOBinaryDecoder($io);
    if (!$this->handshake) {
      $protocolWrapper->readRequestHandshake($decoder);
      $this->handshake = true;
    }
    $protocolWrapper->readMetadata($decoder);
    $method = $decoder->read_string();
    
    return $protocolWrapper->readRequest($decoder, $method);
  }
}
