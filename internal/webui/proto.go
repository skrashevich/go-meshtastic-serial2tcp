package webui

import (
	"fmt"

	meshtasticpb "github.com/skrashevich/go-meshtastic-serial2tcp/internal/meshtastic"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const maxEventJSON = 2048

func marshalProtoJSON(msg proto.Message) string {
	return marshalProtoJSONLimit(msg, maxEventJSON)
}

func marshalProtoJSONFull(msg proto.Message) string {
	return marshalProtoJSONLimit(msg, 0)
}

func marshalProtoJSONLimit(msg proto.Message, maxLen int) string {
	data, err := protojson.MarshalOptions{
		UseProtoNames: true,
		Multiline:     false,
	}.Marshal(msg)
	if err != nil {
		return ""
	}
	if maxLen > 0 && len(data) > maxLen {
		return string(data[:maxLen]) + fmt.Sprintf(" ...(truncated %d chars)", len(data)-maxLen)
	}
	return string(data)
}

func protoVariantLabel(frame *meshtasticpb.FromRadio) string {
	switch v := frame.GetPayloadVariant().(type) {
	case *meshtasticpb.FromRadio_ConfigCompleteId:
		return fmt.Sprintf("ConfigCompleteId=0x%x", v.ConfigCompleteId)
	case *meshtasticpb.FromRadio_Rebooted:
		return fmt.Sprintf("rebooted=%v", v.Rebooted)
	case *meshtasticpb.FromRadio_Packet:
		if p := v.Packet; p != nil {
			return fmt.Sprintf("Packet id=0x%x from=0x%x", p.GetId(), p.GetFrom())
		}
		return "Packet"
	case *meshtasticpb.FromRadio_Channel:
		if v.Channel != nil {
			return fmt.Sprintf("Channel index=%d", v.Channel.GetIndex())
		}
		return "Channel"
	case *meshtasticpb.FromRadio_Config:
		return "Config"
	case *meshtasticpb.FromRadio_ModuleConfig:
		return "ModuleConfig"
	case *meshtasticpb.FromRadio_MyInfo:
		return "MyInfo"
	case *meshtasticpb.FromRadio_NodeInfo:
		return "NodeInfo"
	default:
		if frame.GetPayloadVariant() != nil {
			return fmt.Sprintf("%T", frame.GetPayloadVariant())
		}
	}
	return ""
}

func protoVariantLabelTo(frame *meshtasticpb.ToRadio) string {
	switch v := frame.GetPayloadVariant().(type) {
	case *meshtasticpb.ToRadio_WantConfigId:
		return fmt.Sprintf("WantConfigId=0x%x", v.WantConfigId)
	case *meshtasticpb.ToRadio_Disconnect:
		return "Disconnect"
	case *meshtasticpb.ToRadio_Packet:
		if p := v.Packet; p != nil {
			return fmt.Sprintf("Packet id=0x%x from=0x%x", p.GetId(), p.GetFrom())
		}
		return "Packet"
	default:
		if frame.GetPayloadVariant() != nil {
			return fmt.Sprintf("%T", frame.GetPayloadVariant())
		}
	}
	return ""
}
