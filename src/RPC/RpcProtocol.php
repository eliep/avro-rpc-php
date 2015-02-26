<?php

namespace Avro\RPC;

use Avro\RPC\RpcProtocolHelper;
use Avro\RPC\RpcServer;
use Avro\RPC\RpcClient;
use Avro\RPC\RpcTransport;

abstract class RpcProtocol {

  protected static $protocol = null;
  public $protocolHelper = null;
  protected $transport = null;

  
  public function __construct($transport) {
    $this->protocolHelper = new RpcProtocolHelper($this->getJsonProtocol());
    $this->transport = $transport;
  }
  
  abstract public function getJsonProtocol();
  
  public function renew() {
    $this->transport = RpcTransport::renew($this->transport);
    return $this;
  }
  
  protected function genericRequest($params) {
    $this->transport->send($this->protocolHelper, "send", $params);
    $response = $this->transport->read($this->protocolHelper, "send");
    //$this->renew();
    return $response;
  }
  
  protected function genericResponse($callback) {
    while (true) {
      $params = $this->transport->read($this->protocolHelper, $method);
      $result = $callback($params);
      $this->transport->send($this->protocolHelper, $method, $result);
    }
  }
  
  public static function getClient($host, $port) {
    $client = RpcTransport::getTransport(RpcTransport::CLIENT, $host, $port);
    if (is_null(self::$protocol)) {
      $caller = get_called_class();
      self::$protocol = new $caller($client);
    }
    return self::$protocol;
  }
  
  public static function getServer($host, $port) {
    $server = RpcTransport::getTransport(RpcTransport::SERVER, $host, $port);
    if (is_null(self::$protocol)) {
      $caller = get_called_class();
      self::$protocol = new $caller($server);
    }
    return self::$protocol;
  }
  
}