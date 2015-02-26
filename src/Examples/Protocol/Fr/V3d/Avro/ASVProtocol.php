<?php

namespace Avro\Examples\Protocol\Fr\V3d\Avro;

use Avro\RPC\RpcProtocol;

class ASVProtocol extends RpcProtocol {
  
  private $jsonProtocol =
<<<PROTO
{"protocol":"ASV","namespace":"examples.protocol.fr.v3d.avro","types":[{"type":"record","name":"Asv","fields":[{"name":"a","type":"int"},{"name":"s","type":"string"},{"name":"v","type":"string"}]},{"type":"record","name":"Attention","fields":[{"name":"status","type":"string"}]}],"messages":{"send":{"request":[{"name":"message","type":"Asv"}],"response":"Attention"}}}
PROTO;


  public function getJsonProtocol() { return $this->jsonProtocol; }
  
    
  public function send($message) {
    return $this->genericRequest(array($message));
  }
    

    
  public function sendImpl($callback) {
    return $this->genericResponse($callback);
  }
    
  
}