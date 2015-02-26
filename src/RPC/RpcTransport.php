<?php

namespace Avro\RPC;


class RpcTransport {
  
  const CLIENT = "Client";
  const SERVER = "Server";
  
  protected $socket;
  protected $host;
  protected $port;
  protected $handshake = false;
  
  public function __construct($host, $port) {
    $this->host = $host;
    $this->port = $port;
  }
  
  public function getHost() { return $this->host; }
  public function getPort() { return $this->port; }
  public function close() { socket_close($this->socket); }
  
  public function getType() {
    return ($this instanceof RpcClient) ? self::CLIENT : self::SERVER;
  }
  
  public static function getTransport($type, $host, $port) {
    switch ($type) {
      case self::CLIENT:
        $transport = new RpcClient($host, $port);
        break;
      case self::SERVER:
        $transport = new RpcServer($host, $port);
        break;
    }
    return $transport;
  }
  
  public static function renew(RpcTransport $transport) {
    $transport->close();
    return self::getTransport($transport->getType(), $transport->getHost(), $transport->getPort());
  }
}
