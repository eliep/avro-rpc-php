<?php

namespace Avro\Examples\Protocol\Fr\V3d\Avro;

use Avro\RPC\RpcProtocol;

class ASVProtocol extends RpcProtocol {
  
  private $jsonProtocol =
<<<PROTO
{"namespace":"examples.protocol.fr.v3d.avro","protocol":"ASV","types":[{"type":"record","name":"Attention","fields":[{"name":"status","type":"string"}]},{"type":"record","name":"Asv","fields":[{"name":"a","type":"int"},{"name":"s","type":"string"},{"name":"v","type":"string"}]},{"name":"Curse","type":"error","fields":[{"name":"problem","type":"string"}]},{"name":"BigCurse","type":"error","fields":[{"name":"bigproblem","type":"string"}]}],"messages":{"send":{"doc":"Say Hello","request":[{"name":"message","type":"Asv"}],"response":"Attention","errors":["Curse","BigCurse"]},"notify":{"doc":"Say Hello","request":[{"name":"message","type":"Asv"}],"one-way":true}}}
PROTO;

  
  public function getJsonProtocol() { return $this->jsonProtocol; }
  public function getMd5() { return md5($this->jsonProtocol, true); }
  
    
  public function send($message) {
    return $this->genericRequest(array($message));
  }
    
    
  public function notify($message) {
    return $this->genericRequest(array($message));
  }
    

    
  public function sendImpl($callback) {
    return $this->genericResponse($callback);
  }
    
    
  public function notifyImpl($callback) {
    return $this->genericResponse($callback);
  }
    
  
}