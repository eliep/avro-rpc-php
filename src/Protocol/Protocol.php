<?php

namespace Avro\Protocol;

use Avro\RPC\AvroProtocolWrapper;
use Avro\RPC\RpcServer;
use Avro\RPC\RpcClient;

abstract class Protocol {

  protected static $protocol = null;
  public $protocolWrapper = null;
  protected $transport = null;

  public static function getClient($host, $port) {
    $client = new RpcClient($host, $port);
    if (is_null(self::$protocol)) {
      self::$protocol = new MailProtocol($client);
    }
    return self::$protocol;
  }
  
  public static function getServer($host, $port) {
    $server = new RpcServer($host, $port);
    if (is_null(self::$protocol)) {
      self::$protocol = new MailProtocol($server);
    }
    return self::$protocol;
  }
  
  public function __construct($transport) {
    $this->protocolWrapper = new AvroProtocolWrapper($this->getJsonProtocol());
    $this->transport = $transport;
  }
  
  abstract public function getJsonProtocol();
  
  protected function abstractRequest($params) {
    $this->transport->send($this->protocolWrapper, "send", $params);
    return $this->transport->read($this->protocolWrapper, "send");
  }
  
  protected function abstractResponse($callback) {
    while (true) {
      $params = $this->transport->read($this->protocolWrapper, $method);
      $result = $callback($params);
      $this->transport->send($this->protocolWrapper, $method, $result);
    }
  }
  
}