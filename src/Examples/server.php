<?php



require_once __DIR__."/../../lib/avro.php";
require_once __DIR__."/../../vendor/autoload.php";

use \Avro\Examples\Protocol\Fr\V3d\Avro\ASVProtocol;
$p = new ASVProtocol(null);

class AsvResponder extends Responder {
  public function invoke( $local_message, $request) {
    echo json_encode($request)."\n";
    return array("status" => "tti");
  }
}

$server = new SocketServer('127.0.0.1', 1432, new AsvResponder(AvroProtocol::parse($p->getJsonProtocol())));
$server->start();


/*
require_once __DIR__."/../../vendor/autoload.php";

use Avro\Examples\Protocol\Fr\V3d\Avro\ASVProtocol;


$protocol = ASVProtocol::getServer('127.0.0.1', 1424);
$protocol->sendImpl(function($params) {
  $msg = $params[0];
  return array("status"=>"toto");
});
*/