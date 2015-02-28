<?php

namespace Avro\Examples\Protocol\Fr\V3d\Avro;

use Avro\RPC\RpcProtocol;

class ASVProtocol extends RpcProtocol {
  
  private $jsonProtocol =
<<<PROTO
{"namespace":"examples.protocol.fr.v3d.avro","protocol":"ASV","types":[{"type":"record","name":"Attention","fields":[{"name":"status","type":"string"}]},{"type":"record","name":"Asv","fields":[{"name":"a","type":"int"},{"name":"s","type":"string"},{"name":"v","type":"string"}]}],"messages":{"send":{"request":[{"name":"message","type":"Asv"}],"response":"Attention"}}}
PROTO;


  private $md5 = ",å-¬œŒåƒ«d_Ôð\Ìä";
  
  public function getJsonProtocol() { return $this->jsonProtocol; }
  public function getMd5() { return $this->md5; }
  
    
  public function send($message) {
    return $this->genericRequest($message);
  }
    

    
  public function sendImpl($callback) {
    return $this->genericResponse($callback);
  }
    
  
}