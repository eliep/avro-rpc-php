<?php

require_once __DIR__."/../../vendor/autoload.php";

use \Avro\Examples\Protocol\Fr\V3d\Avro\ASVProtocol;

$datum = array(
  array("a" => 20, "s" => "f", "v" => "there"),
  array("a" => 20, "s" => "f", "v" => "there"),
  array("a" => 20, "s" => "h", "v" => "there"),
  array("a" => 20, "s" => "f", "v" => "there"),
  array("a" => 50, "s" => "f", "v" => "there")
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
}
