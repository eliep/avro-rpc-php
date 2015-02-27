<?php

namespace Avro\RPC;


class RpcTransport {
  
  const CLIENT = "Client";
  const SERVER = "Server";
  
  private static $serial = 0;
  
  protected $socket;
  protected $host;
  protected $port;
  protected $handshake = false;
  protected $frame_length = 1024;
  
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
  
  public function write($socket, $binary) {
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
    $x = socket_write ($socket, $header, strlen($header));
    foreach ($frames as $frame) {
      $msg = pack("N", strlen($frame)).$frame;
      //socket_send ( $this->socket, $msg, strlen($msg), 0);
      $x = socket_write ( $socket, $msg, strlen($msg));
    }
  }
  
  
  public function read($socket) {
    socket_recv ( $socket , $buf , 8 , MSG_WAITALL );
    $frame_count = unpack("Nserial/Ncount", $buf);
    $frame_count = $frame_count["count"];
    $binary = "";
    for ($i = 0; $i < $frame_count; $i++ ) {
      socket_recv ( $socket , $buf , 4 , MSG_WAITALL );
      $frame_size = unpack("Nsize", $buf);
      $frame_size = $frame_size["size"];
      socket_recv ( $socket , $buf , $frame_size , MSG_WAITALL );
      $binary .= $buf;
    }
    
    return $binary;
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
