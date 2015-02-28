<?php

require_once __DIR__."/../../lib/avro.php";
require_once __DIR__."/../../vendor/autoload.php";

use \Avro\Examples\Protocol\Fr\V3d\Avro\ASVProtocol;
$p = new ASVProtocol(null);

$client = new SocketTransceiver('127.0.0.1', 1432);
$requestor = new Requestor(AvroProtocol::parse($p->getJsonProtocol()), $client);
while (true) {
echo json_encode($requestor->request('send', array("message" => array("a" => 20, "s" => "f", "v" => "there"))))."\n";
echo json_encode($requestor->request('send', array("message" => array("a" => 20, "s" => "f", "v" => "there"))))."\n";
sleep(1);
}
# cleanup
$client->close();

//var_dump($handshake_requestor_reader);
/*
require_once __DIR__."/../../vendor/autoload.php";

use \Avro\Examples\Protocol\Fr\V3d\Avro\ASVProtocol;

$datum = array(
  array("message" => array("a" => 20, "s" => "f", "v" => "there"))
);
//new ASVProtocol(null);

$protocol = ASVProtocol::getClient('127.0.0.1', 1420);//192.168.100.109
foreach ($datum as $data) {
  try {
    $response = $protocol->send($data);
  } catch (\Avro\RPC\RpcResponseException $e) {
    $response = $e->getMessage();
  }
  echo json_encode($response)."\n";
}*/