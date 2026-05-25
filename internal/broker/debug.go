package broker

import (
	"fmt"
	"log"

	meshtasticpb "github.com/skrashevich/go-meshtastic-serial2tcp/internal/meshtastic"
	"google.golang.org/protobuf/proto"
)

func (b *Broker) logConfig(format string, args ...any) {
	if !b.debug {
		return
	}
	log.Printf("[config] "+format, args...)
}

// protoVariantLabel returns a short label for config-handshake and other
// high-signal protobuf variants (empty when nothing notable).
func protoVariantLabel(msg proto.Message) string {
	switch m := msg.(type) {
	case *meshtasticpb.ToRadio:
		switch v := m.GetPayloadVariant().(type) {
		case *meshtasticpb.ToRadio_WantConfigId:
			return fmt.Sprintf("WantConfigId=0x%x", v.WantConfigId)
		case *meshtasticpb.ToRadio_Disconnect:
			return "Disconnect"
		case *meshtasticpb.ToRadio_Packet:
			if p := v.Packet; p != nil {
				return fmt.Sprintf("Packet id=0x%x from=0x%x", p.GetId(), p.GetFrom())
			}
			return "Packet"
		}
	case *meshtasticpb.FromRadio:
		switch v := m.GetPayloadVariant().(type) {
		case *meshtasticpb.FromRadio_ConfigCompleteId:
			return fmt.Sprintf("ConfigCompleteId=0x%x", v.ConfigCompleteId)
		case *meshtasticpb.FromRadio_Rebooted:
			return fmt.Sprintf("rebooted=%v", v.Rebooted)
		case *meshtasticpb.FromRadio_MyInfo:
			return "MyInfo"
		case *meshtasticpb.FromRadio_Config:
			if v.Config != nil {
				return fmt.Sprintf("Config %T", v.Config.GetPayloadVariant())
			}
			return "Config"
		case *meshtasticpb.FromRadio_ModuleConfig:
			if v.ModuleConfig != nil {
				return fmt.Sprintf("ModuleConfig %T", v.ModuleConfig.GetPayloadVariant())
			}
			return "ModuleConfig"
		case *meshtasticpb.FromRadio_Channel:
			if v.Channel != nil {
				return fmt.Sprintf("Channel index=%d", v.Channel.GetIndex())
			}
			return "Channel"
		case *meshtasticpb.FromRadio_NodeInfo:
			if v.NodeInfo != nil {
				return fmt.Sprintf("NodeInfo num=0x%x", v.NodeInfo.GetNum())
			}
			return "NodeInfo"
		case *meshtasticpb.FromRadio_Metadata:
			return "Metadata"
		case *meshtasticpb.FromRadio_DeviceuiConfig:
			return "DeviceuiConfig"
		case *meshtasticpb.FromRadio_Packet:
			if p := v.Packet; p != nil {
				return fmt.Sprintf("Packet id=0x%x from=0x%x", p.GetId(), p.GetFrom())
			}
			return "Packet"
		}
	}
	return ""
}

func cacheUpdateLabel(frame *meshtasticpb.FromRadio) string {
	switch frame.GetPayloadVariant().(type) {
	case *meshtasticpb.FromRadio_MyInfo,
		*meshtasticpb.FromRadio_NodeInfo,
		*meshtasticpb.FromRadio_Config,
		*meshtasticpb.FromRadio_ModuleConfig,
		*meshtasticpb.FromRadio_Channel,
		*meshtasticpb.FromRadio_Metadata,
		*meshtasticpb.FromRadio_DeviceuiConfig:
		return protoVariantLabel(frame)
	default:
		return ""
	}
}

func formatCacheStats(myInfo bool, configs, moduleConfigs, channels, nodeInfo int, metadata, deviceUI bool) string {
	return fmt.Sprintf("myInfo=%t configs=%d moduleConfigs=%d channels=%d nodeInfo=%d metadata=%t deviceUI=%t",
		myInfo, configs, moduleConfigs, channels, nodeInfo, metadata, deviceUI)
}

func (c *configCache) describe() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return formatCacheStats(c.myInfo != nil, len(c.config), len(c.moduleConfig), len(c.channels), len(c.nodeInfo),
		c.metadata != nil, c.deviceUI != nil)
}

func describeCacheSnapshot(snap cacheSnapshot) string {
	return formatCacheStats(len(snap.myInfo) > 0, len(snap.configs), len(snap.moduleConfig), len(snap.channels), len(snap.nodeInfo),
		len(snap.metadata) > 0, len(snap.deviceUI) > 0)
}
