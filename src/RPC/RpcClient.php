<?php

namespace Avro\RPC;


class RpcClient extends RpcTransport {
  
  
  public function __construct($host, $port) {
    parent::__construct($host, $port);
    $this->socket = socket_create(AF_INET, SOCK_STREAM, 0);
    socket_connect($this->socket , $host , $port);
  }
  
  public function send($protocolWrapper, $method, $params) {
    $binary = $this->encode($protocolWrapper, $method, $params);
    $this->write($this->socket, $binary);
  }
  
  public function receive($protocolWrapper, $method) {
    $binary = $this->read($this->socket);
    return $this->decode($protocolWrapper, $method, $binary);
  }
  
  public function encode($protocolWrapper, $method, $params) {
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
  
  public function decode($protocolWrapper, $method, $binary) {
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
}
