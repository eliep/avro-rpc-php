<?php
require_once __DIR__."/../../vendor/autoload.php";

use Avro\Examples\Protocol\Fr\V3d\Avro\ASVProtocol;


$protocol = ASVProtocol::getServer('127.0.0.1', 1420);
$protocol->sendImpl(function($params) {
  $msg = $params[0];
  return array("status"=>"toto");
});
