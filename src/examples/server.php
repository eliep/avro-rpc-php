<?php
require_once __DIR__."/../../vendor/autoload.php";

use Avro\Protocol\MailProtocol;


$protocol = MailProtocol::getServer('localhost', 1421);
$protocol->sendImpl(function($params) {
  $msg = $params[0];
  echo json_encode($params)."\n";
  return array("body"=>"toto");
});
