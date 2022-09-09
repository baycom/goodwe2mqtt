var util=require('util');
var mqtt=require('mqtt');
var ModbusRTU = require("modbus-serial");
var Parser = require('binary-parser').Parser;
const commandLineArgs = require('command-line-args')

const optionDefinitions = [
	{ name: 'mqtthost', alias: 'm', type: String, defaultValue: "web" },
	{ name: 'mqttclientid', alias: 'c', type: String, defaultValue: "goodwe1Client" },
	{ name: 'inverterhost', alias: 'i', type: String, defaultValue:  "goodwe1"},
	{ name: 'address', alias: 'a', type: Number , defaultValue: 1}
  ];

const options = commandLineArgs(optionDefinitions)

var GWSerialNumber;
var modbusClient = new ModbusRTU();

console.log("MQTT host         : " + options.mqtthost);
console.log("MQTT Client ID    : " + options.mqttclientid);
console.log("GoodWe host       : " + options.inverterhost);
console.log("GoodWe MODBUS addr: " + options.address);

var MQTTclient = mqtt.connect("mqtt://"+options.mqtthost,{clientId: options.mqttclientid});
	MQTTclient.on("connect",function(){	
	console.log("MQTT connected");
})

MQTTclient.on("error",function(error){
		console.log("Can't connect" + error);
		process.exit(1)
	});

const payloadParser = new Parser()
	.seek((0x891F-0x891C)*2)
	.uint16be('PV1Voltage', { formatter: (x) => {return x/10.0;}})
	.uint16be('PV1Current', { formatter: (x) => {return x/10.0;}})
	.uint32be('PV1Power')
	.uint16be('PV2Voltage', { formatter: (x) => {return x/10.0;}})
	.uint16be('PV2Current', { formatter: (x) => {return x/10.0;}})
	.uint32be('PV2Power')
	.uint16be('PV3Voltage', { formatter: (x) => {return x/10.0;}})
	.uint16be('PV3Current', { formatter: (x) => {return x/10.0;}})
	.uint32be('PV3Power')
	.uint16be('PV4Voltage', { formatter: (x) => {return x/10.0;}})
	.uint16be('PV4Current', { formatter: (x) => {return x/10.0;}})
	.uint32be('PV4Power')
	.uint32be('PVWorkMode')
	.uint16be('OnGridL1Voltage', { formatter: (x) => {return x/10.0;}})
	.int16be('OnGridL1Current', { formatter: (x) => {return x/10.0;}})
	.uint16be('OnGridL1Frequency', { formatter: (x) => {return x/100.0;}})
	.int32be('OnGridL1Power')
	.uint16be('OnGridL2Voltage', { formatter: (x) => {return x/10.0;}})
	.int16be('OnGridL2Current', { formatter: (x) => {return x/10.0;}})
	.uint16be('OnGridL2Frequency', { formatter: (x) => {return x/100.0;}})
	.int32be('OnGridL2Power')
	.uint16be('OnGridL3Voltage', { formatter: (x) => {return x/10.0;}})
	.int16be('OnGridL3Current', { formatter: (x) => {return x/10.0;}})
	.uint16be('OnGridL3Frequency', { formatter: (x) => {return x/100.0;}})
	.int32be('OnGridL3Power')
	.int16be('GridMode')
	.int32be('TotalInverterPower')
	.seek((0x8944-0x8943)*2)
	.int16be('ActivePower')
	.seek((0x8949-0x8945)*2)
	.uint16be('BackupL1Voltage', { formatter: (x) => {return x/10.0;}})
	.uint16be('BackupL1Current', { formatter: (x) => {return x/10.0;}})
	.uint16be('BackupL1Frequency', { formatter: (x) => {return x/100.0;}})
	.seek((0x894D-0x894C)*2)
	.int32be('BackupL1Power')
	.uint16be('BackupL2Voltage', { formatter: (x) => {return x/10.0;}})
	.uint16be('BackupL2Current', { formatter: (x) => {return x/10.0;}})
	.uint16be('BackupL2Frequency', { formatter: (x) => {return x/100.0;}})
	.seek((0x8953-0x8952)*2)
	.int32be('BackupL2Power')
	.uint16be('BackupL3Voltage', { formatter: (x) => {return x/10.0;}})
	.uint16be('BackupL3Current', { formatter: (x) => {return x/10.0;}})
	.uint16be('BackupL3Frequency', { formatter: (x) => {return x/100.0;}})
	.seek((0x8959-0x8958)*2)
	.int32be('BackupL3Power')
	.uint32be('LoadL1')
	.uint32be('LoadL2')
	.uint32be('LoadL3')
	.int32be('TotalBackupPower')
	.int32be('TotalLoadPower')
	.uint16be('UPSLoadPercent')
	.uint16be('AirTemperature', { formatter: (x) => {return x/10.0;}})
	.uint16be('ModuleTemperature', { formatter: (x) => {return x/10.0;}})
	.uint16be('RadiatorTemperature', { formatter: (x) => {return x/10.0;}})
	.uint16be('FunctionBitValue')
	.uint16be('BUSVoltage', { formatter: (x) => {return x/10.0;}})
	.uint16be('NBUSVoltage', { formatter: (x) => {return x/10.0;}})
	.uint16be('BatteryVoltage', { formatter: (x) => {return x/10.0;}})
	.int16be('BatteryCurrent', { formatter: (x) => {return x/10.0;}})
	.seek(2)
	.int16be('BatteryPower')
	.uint16be('BatteryMode')
	.uint16be('WarningCode')
	.uint16be('CountryCode')
	.uint16be('WorkMode')
	.uint16be('OperationMode')
	.uint32be('ErrorMessage')
	.uint32be('TotalPVGeneration', { formatter: (x) => {return x/10.0;}})
	.uint32be('TodayPVGeneration', { formatter: (x) => {return x/10.0;}})
	.uint32be('ETotal', { formatter: (x) => {return x/10.0;}})
	.uint32be('TotalHours')
	.uint16be('EDaySell', { formatter: (x) => {return x/10.0;}})
	.uint32be('ETotalBuy', { formatter: (x) => {return x/10.0;}})
	.uint16be('EDayBuy', { formatter: (x) => {return x/10.0;}})
	.uint32be('ETotalLoad', { formatter: (x) => {return x/10.0;}})
	.uint16be('ELoadDay', { formatter: (x) => {return x/10.0;}})
	.uint32be('EBatteryCharge', { formatter: (x) => {return x/10.0;}})
	.uint16be('EChargeDay', { formatter: (x) => {return x/10.0;}})
	.uint32be('EBatteryDischarge', { formatter: (x) => {return x/10.0;}})
	.uint16be('EDischargeDay', { formatter: (x) => {return x/10.0;}})
	.uint16be('BattStrings')
	.uint16be('CPLDWarningCode')
	.uint32be('wChargerCtrlFlg')
	.uint16be('DerateFlag')
	.uint32be('DerateFrozenPower')
	.uint32be('DiagStatusH')
	.uint32be('DiagStatusL')
	;
function getPayload(data) {
	return payloadParser.parse(data);
}

function getSN() {
	modbusClient.readHoldingRegisters(0x88BB, 8, function(err, vals) {
		if(err) {
			console.log(util.inspect(err));
                        process.exit(1);
		} else {		
			GWSerialNumber = new String(vals.buffer);
			console.log(GWSerialNumber);
			setTimeout(MODBUSintervalFunc, 1000);
		}
	});
}

function sendMqtt(data) {
	if(GWSerialNumber) {
		MQTTclient.publish('GoodWe/' + GWSerialNumber, JSON.stringify(data));
	}	
}

function MODBUSintervalFunc () {
	modbusClient.readHoldingRegisters(0x891C, 123, function(err, vals) {
		if(err) {
			console.log("readHoldingRegisters: " + util.inspect(err));
			process.exit(1);
		} else {
			var gwState = getPayload(vals.buffer);
			console.log(util.inspect(gwState));
			sendMqtt(gwState);
			setTimeout(MODBUSintervalFunc, 10000);
		}
	});
}

modbusClient.connectTCP(options.inverterhost, { port: 502 });
modbusClient.setID(options.address);
modbusClient.setTimeout(1000);
setTimeout(getSN, 1000);
