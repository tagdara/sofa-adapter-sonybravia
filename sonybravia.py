#!/usr/bin/python3

import sys, os
# Add relative paths for the directory where the adapter is located as well as the parent
sys.path.append(os.path.dirname(__file__))
sys.path.append(os.path.join(os.path.dirname(__file__),'../../base'))

from sofabase import sofabase, adapterbase, configbase
import devices

import math
import random
from collections import namedtuple
import json
#import definitions
import asyncio
import aiohttp
import xml.etree.ElementTree as et
from collections import defaultdict
import struct
import socket
import urllib.request


class BroadcastProtocol:

    def __init__(self, loop, log, keyphrases=[], returnmessage=None):
        self.log=log
        self.loop = loop
        self.keyphrases=keyphrases
        self.returnMessage=returnmessage


    def connection_made(self, transport):
        self.transport = transport
        sock = transport.get_extra_info("socket")
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self.log.info('.. ssdp now listening')


    def datagram_received(self, data, addr):
        data=data.decode()
        for phrase in self.keyphrases:
            if data.find(phrase)>-1:
                if data.find('upnp:rootdevice')>-1:
                    self.processUPNPevent(data)
                #return str(data)
            #else:
             #   self.log.info('>> not the right ssdp: %s' % (data))


    def broadcast(self, data):
        self.log.info('>> ssdp/broadcast %s' % data)
        self.transport.sendto(data.encode(), ('192.168.0.255', 9000))


    def etree_to_dict(self, t):
        
        d = {t.tag: {} if t.attrib else None}
        children = list(t)
        if children:
            dd = defaultdict(list)
            for dc in map(self.etree_to_dict, children):
                for k, v in dc.items():
                    dd[k].append(v)
            d = {t.tag: {k: v[0] if len(v) == 1 else v for k, v in dd.items()}}
        if t.attrib:
            d[t.tag].update(('@' + k, v) for k, v in t.attrib.items())
        if t.text:
            text = t.text.strip()
            if children or t.attrib:
                if text:
                    d[t.tag]['#text'] = text
            else:
                d[t.tag] = text
        return d


    def processUPNPevent(self, event):   

        try:
            asyncio.ensure_future(self.returnMessage(event))

        except:
            self.log.info("Error processing UPNP Event: %s " % upnpxml,exc_info=True)


class sony_rest():

    def __init__(self, log=None, config=None):
        self.config=config
        self.log=log
        self.tv_timeout=5
    
    async def remoteControl(self, params):

        method = "POST"
        url="sony/IRCC"
        service='urn:schemas-sony-com:service:IRCC:1#X_SendIRCC'

        soap = 	'<?xml version="1.0"?>'\
            '<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" s:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/">'\
            '<s:Body>'\
            '<u:X_SendIRCC xmlns:u="urn:schemas-sony-com:service:IRCC:1">'\
            '<IRCCCode>%s</IRCCCode>'\
            '</u:X_SendIRCC>'\
            '</s:Body>'\
            '</s:Envelope>' % (params)

        headers = {
            'Host': "%s:%s" % (self.config.tv_address, self.config.tv_port),
            'Content-length':len(soap),
            'Content-Type':'text/xml; charset="utf-8"',
            'X-Auth-PSK': self.config.tv_preshared_key,
            'SOAPAction':'"%s"' % (service)
            }

        req = urllib.request.Request("http://%s:%s/%s" % (self.config.tv_address, self.config.tv_port, url), data=soap.encode('ascii'), headers=headers)
        req.get_method = lambda: method

        try:
            response = urllib.request.urlopen(req)
        except urllib.HTTPError:
            self.log.error("!! HTTP Error", exc_info=True)
		
        except urllib.URLError:
            self.log.error("!! URL Error", exc_info=True)

        else:
            tree = response.read()
            #self.log.info('<- command Sent: %s' % str(tree))
            return tree


    async def getState(self, section, method, version='1.0', params=[]):
        
        try:
            url = "http://%s/sony/%s" % (self.config.tv_address,section)
            headers={'X-Auth-PSK': self.config.tv_preshared_key}
            command={'id':2, 'method':method, 'version':version}
            if params==[]:
                command['params']=[]
            else:
                command['params']=[params]
            data=json.dumps(command)
            
            timeout = aiohttp.ClientTimeout(total=self.tv_timeout)
            async with aiohttp.ClientSession(timeout=timeout) as client:
                response=await client.post(url, data=data, headers=headers)
                result=await response.read()
                result=json.loads(result.decode())
                
                if 'result' in result:
                    result=result['result']
                    return result
                if 'results' in result:
                    result=result['results']
                    for subresult in result:
                        self.log.info('Multi-result: %s' % subresult)
                    return result
                elif 'error' in result:
                    if 'Display Is Turned off' in result['error']:
                        pass
                    elif 'Illegal State' in result['error']:
                        pass

                    else:
                        self.log.error('Error result: %s %s' % (result, data))
                    return {}
                else:
                    self.log.info('Result has no result: %s' % result)
                    return result
                    
        except concurrent.futures._base.CancelledError:
            self.log.error('!! Error sending command to TV (cancelled) - %s/%s %s' % (section, method, params) )
            return {}

        except aiohttp.client_exceptions.ClientConnectorError:
            self.log.error('!! Error sending command to TV (could not connect, likely DNS or IP related) - %s/%s %s' % (section, method, params) )
            return {}
                
        except:
            self.log.error('!! Error sending command to TV - %s/%s %s' % (section, method, params), exc_info=True)
            return {}



class sonybravia(sofabase):

    class adapter_config(configbase):
    
        def adapter_fields(self):
            self.hdmi_port_names=self.set_or_default('hdmi_port_names', default={})
            self.tv_address=self.set_or_default("tv_address", mandatory=True)
            self.tv_port=self.set_or_default("tv_port", default=80)
            self.tv_preshared_key=self.set_or_default("tv_preshared_key", mandatory=True)
            self.ssdpkeywords=self.set_or_default("ssdpkeywords", default=[self.tv_address, "bravia"])

    class EndpointHealth(devices.EndpointHealth):

        @property            
        def connectivity(self):
            return 'OK'

    class PowerController(devices.PowerController):

        @property            
        def powerState(self):
            return "ON" if self.nativeObject['PowerStatus']['status']=="active" else "OFF"

        async def TurnOn(self, correlationToken=''):
            try:
                sysinfo=await self.adapter.tv.getState('system', 'setPowerStatus', params={"status":True})
                await self.adapter.getUpdate()
                return await self.adapter.dataset.generateResponse(self.device.endpointId, correlationToken)
            except:
                self.adapter.log.error('!! Error during TurnOn', exc_info=True)
                return None
        
        async def TurnOff(self, correlationToken=''):
            try:
                sysinfo=await self.adapter.tv.getState('system', 'setPowerStatus', params={"status":False})
                await self.adapter.getUpdate()
                return await self.adapter.dataset.generateResponse(self.device.endpointId, correlationToken)
            except:
                self.adapter.log.error('!! Error during TurnOff', exc_info=True)
                return None

    class AudioModeController(devices.ModeController):

        @property            
        def mode(self):
            try:
                for item in self.nativeObject['SoundSettings']:
                    if item['target']=='outputTerminal':
                        otmode="%s.%s" % (self.name,item['currentValue'])
                        return otmode
                        self.log.info('## %s vs %s' % (otmode, self._supportedModes))
                        for mode in self._supportedModes:
                            if otmode==[mode]:
                                return otmode
                return ""
            except KeyError:
                return ""
                #self.adapter.log.error('Error checking mode status - no value present: %s' % self.nativeObject)
            except:
                self.adapter.log.error('Error checking mode status', exc_info=True)
            return ""

        async def SetMode(self, payload, correlationToken=''):
            try:
                if 'mode' in payload:
                    mode=payload['mode'].split('.')[1]
                    if mode in self._supportedModes:
                        if self.nativeObject['PowerStatus']['status']!="active":
                            self.log.warn('!! Warning: wont try to change audio mode while tv is off')
                        else:
                            self.log.info('.. setting tv setSoundSettings to %s' % mode)
                            sysinfo=await self.adapter.tv.getState('audio','setSoundSettings',version="1.1",params={"settings": [{ "value": mode, "target": "outputTerminal"} ] })
                            await self.adapter.getUpdate()
                        return await self.adapter.dataset.generateResponse(self.device.endpointId, correlationToken)     
                    self.log.error('!! error - did not find mode %s in %s/%s' % (payload, self.name, self._supportedModes))
            except:
                self.adapter.log.error('Error setting mode status %s' % payload, exc_info=True)
            return {}

    class PowerSavingModeController(devices.ModeController):

        @property            
        def mode(self):
            try:
                otmode="%s.%s" % (self.name,self.nativeObject['PowerSavingMode']['mode'])
                return otmode
                self.log.info('## %s vs %s' % (otmode, self._supportedModes))
                for mode in self._supportedModes:
                    if otmode==[mode]:
                        return otmode
                return ""
            except KeyError:
                return ""
                #self.adapter.log.error('Error checking mode status - no value present: %s' % self.nativeObject)
            except:
                self.adapter.log.error('Error checking mode status', exc_info=True)
            return ""

        async def SetMode(self, payload, correlationToken=''):
            try:
                if 'mode' in payload:
                    mode=payload['mode'].split('.')[1]
                    if mode in self._supportedModes:
                        if self.nativeObject['PowerStatus']['status']!="active":
                            self.log.warn('!! Warning: wont try to change power saving mode while tv is off')
                        else:
                            self.log.info('.. setting tv setPowerSavingMode to %s' % mode)
                            sysinfo=await self.adapter.tv.getState('system','setPowerSavingMode',version="1.0",params={"mode": mode})
                            await self.adapter.getUpdate()
                        return await self.adapter.dataset.generateResponse(self.device.endpointId, correlationToken)     
                    self.log.error('!! error - did not find mode %s in %s/%s' % (payload, self.name, self._supportedModes))
            except:
                self.adapter.log.error('Error setting mode status %s' % payload, exc_info=True)
            return {}

                    

    class InputController(devices.InputController):

        @property            
        def input(self):
            try:
                return self.adapter.parse_input_name(self.nativeObject)
            except KeyError:
                if self.nativeObject['PowerStatus']['status']=="active":
                    return 'Android TV'
                else:
                    return "Off"
            except:
                self.adapter.log.error('Error checking input status', exc_info=True)
                return "Off"
                    
        async def SelectInput(self,payload, correlationToken=''):
            try:
                if payload['input']=='Home':
                    sysinfo=await self.adapter.tv.remoteControl(self.adapter.findRemoteCode('Home'))
                else:
                    inp=payload['input']
                    for port in self.device.adapter.config.hdmi_port_names:
                        if payload['input']==self.device.adapter.config.hdmi_port_names[port]:
                            inp='extInput:hdmi?port=%s' % port
                            sysinfo=await self.adapter.tv.getState('avContent','setPlayContent',params={"uri":inp})
                            if inp.startswith('extInput:cec'):
                                # takes slightly longer for CEC sources to switch than raw AV inputs
                                await asyncio.sleep(.2)
                await self.adapter.getUpdate()
                return await self.adapter.dataset.generateResponse(self.device.endpointId, correlationToken)
            except:
                self.adapter.log.error('Error in SelectInput', exc_info=True)
                return None

    class SpeakerController(devices.SpeakerController):

        @property            
        def volume(self):
            try:
                for item in self.nativeObject['VolumeInformation']:
                    if item['target']=='speaker':
                        return item['volume']
            except KeyError:
                pass
                #self.adapter.log.error('Error checking mode status - no value present: %s' % self.nativeObject)
            except:
                self.log.error('!! Error during volume check', exc_info=True)
            return 50

        @property            
        def mute(self):
            try:
                for item in self.nativeObject['VolumeInformation']:
                    if item['target']=='speaker':
                        return item['mute']
            except KeyError:
                return False
                #self.adapter.log.error('Error checking mode status - no value present: %s' % self.nativeObject)

            except:
                self.log.error('!! Error during volume mute check', exc_info=True)
            return False

        async def SetVolume(self, payload, correlationToken=''):
            try:
                for item in self.nativeObject['SoundSettings']:
                    if item['target']=='outputTerminal':
                        if item['currentValue']!='speaker':
                            self.log.warning('.! cancelled attempt to set volume while the TV is not in speaker mode')
                            return await self.adapter.dataset.generateResponse(self.device.endpointId, correlationToken)

                for item in self.nativeObject['VolumeInformation']:
                    if item['target']=='speaker':
                        volrange={ 'max':item['maxVolume'], 'min':item['minVolume'] }
                unitconv=(volrange['max']-volrange['min'])/100
                realvol=str(int(float(unitconv* int(payload['volume'])))+volrange['min'])
                # { "method": "setAudioVolume", "id": 601,"params": [{ "volume": "18","target": "speaker"}],"version": "1.0"}
                sysinfo=await self.adapter.tv.getState('audio','setAudioVolume',params={"volume":realvol, "target":"speaker"})
                await self.adapter.getUpdate()
                return await self.adapter.dataset.generateResponse(self.device.endpointId, correlationToken)
            except:
                self.log.error('!! Error during SetVolume', exc_info=True)
                return None

        async def SetMute(self, payload, correlationToken=''):
            try:
                self.log.warn('!! SetMute has not been implemented yet.')
            except:
                self.log.error('!! Error during SetMute', exc_info=True)
                return None

    class RemoteController(devices.RemoteController):

        async def PressRemoteButton(self, payload, correlationToken=''):
            try:
                if self.adapter.findRemoteCode(payload['buttonName']):
                    sysinfo=await self.adapter.tv.remoteControl(self.adapter.findRemoteCode(payload['buttonName']))
                await self.adapter.getUpdate()
                return await self.adapter.dataset.generateResponse(self.device.endpointId, correlationToken)
            except:
                self.adapter.log.error('Error in PressRemoteButton', exc_info=True)
                return None

    class adapterProcess(adapterbase):


        def __init__(self, log=None, dataset=None, notify=None, request=None, loop=None, config=None, **kwargs):
            self.config=config
            self.dataset=dataset

            self.log=log
            self.notify=notify
            self.polltime=5
            if not loop:
                self.loop = asyncio.new_event_loop()
            else:
                self.loop=loop
                
        def make_ssdp_sock(self):
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind(('', 1900))
            group = socket.inet_aton('239.255.255.250')
            mreq = struct.pack('4sL', group, socket.INADDR_ANY)
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)    
            return sock            

        async def processUPNP(self, message):
            try:
                await self.getUpdate()
            except:
                self.log.error('Error processing UPNP: %s' % message, exc_info=True)
            

        async def getInitialData(self):

            systemdata={    'system':       [ { 'interface': 'systemInformation', 'command':'getSystemInformation', 'listitem':0 },
                                              { 'interface': 'remoteCommands', 'command':'getRemoteControllerInfo', 'listitem':1 }],
                            'appControl':   [ { 'interface': 'applications', 'command':'getApplicationList'}]
                        }
            return await self.getStates(systemdata)

        async def getUpdate(self):

            systemdata={    'system':       [ { 'interface': 'power', 'command':'getPowerStatus', 'listitem':0 },
                                                { 'interface': 'system', 'command':'getPowerSavingMode', 'listitem':0 }],
                            'audio':        [ { 'interface':'audio', 'command':'getVolumeInformation', 'listitem':0},
                                                { 'interface':'audio', 'command':'getSoundSettings', 'version':'1.1', 'listitem':0, 'params':{"target": ""}}
                                            ],
                            'avContent':    [ { 'interface':'playingContent', 'command':'getPlayingContentInfo', 'listitem':0 },
                                              { 'interface':'inputStatus', 'command':'getCurrentExternalInputsStatus', 'version':'1.1', 'listitem':0 }]
                        }
                                              
            return await self.getStates(systemdata)


        async def getStates(self, systemdata):
            
            alldata={}
            
            try:
                for category in systemdata:
                    results={}
                    for action in systemdata[category]:
                        cmdver="1.0"
                        if 'version' in action:
                            cmdver=action['version']
                        params=[]
                        if 'params' in action:
                            params=action['params']
                        sysinfo=await self.tv.getState(category, action['command'], version=cmdver, params=params)
                        if sysinfo and 'listitem' in action:
                            sysinfo=sysinfo[action['listitem']]
                            results[action['command'][3:]]=sysinfo
                        if category not in alldata:
                            alldata[category]={}
                        alldata[action['command'][3:]]=sysinfo
                    await self.dataset.ingest({'tv': { self.tvName: results }}, mergeReplace=True)
                return alldata
                
            except:
                self.log.error('error with update',exc_info=True)

        async def getTVname(self):
            
            sysinfo=await self.tv.getState('system','getSystemInformation')
            return sysinfo[0]['name']

        async def start(self):
            try:
                self.input_list=[]
                for port in self.config.hdmi_port_names:
                    self.input_list.append(self.config.hdmi_port_names[port])
            except:
                self.log.error('Error defining port list', exc_info=True)
                
            self.tv=sony_rest(log=self.log, config=self.config)
            self.tvName=await self.getTVname()

            try:
                await self.getInitialData()
                await self.getUpdate()

            except:
                self.log.error('error with update',exc_info=True)

            try:
                sock=self.make_ssdp_sock()
                self.ssdp = self.loop.create_datagram_endpoint(lambda: BroadcastProtocol(self.loop, self.log, self.config.ssdpkeywords, returnmessage=self.processUPNP), sock=sock)
                await self.ssdp
                await self.pollTV()
                
            except:
                self.log.error('error with ssdp',exc_info=True)
           
        async def pollTV(self):
            while True:
                try:
                    #self.log.info("Polling TV")
                    sysinfo=await self.tv.getState('system','getPowerStatus')
                    if sysinfo:
                        await self.dataset.ingest({'tv':  { self.tvName: {'PowerStatus': sysinfo[0]}}})
                    await asyncio.sleep(self.polltime)
                except:
                    self.log.error('Error fetching TV Data', exc_info=True)


        async def addSmartDevice(self, path):
            
            try:
                if path.split("/")[2]=="BRAVIA":
                    return self.addSmartTV(path.split("/")[2], "TV")
                else:
                    self.log.info('Path not adding device: %s' % path)

            except:
                self.log.error('Error defining smart device', exc_info=True)
                return False


        def addSmartTV(self, deviceid, name="Bravia"):
            try:
                nativeObject=self.dataset.nativeDevices['tv'][deviceid]
                if name not in self.dataset.localDevices:
                    if "SystemInformation" in nativeObject and "PowerStatus" in nativeObject:
                        device=devices.alexaDevice('sonybravia/tv/%s' % deviceid, name, displayCategories=['TV'], adapter=self)
                        device.PowerController=sonybravia.PowerController(device=device)
                        device.EndpointHealth=sonybravia.EndpointHealth(device=device)
                        device.InputController=sonybravia.InputController(device=device, inputs=self.input_list)
                        device.RemoteController=sonybravia.RemoteController(device=device)
                        device.SpeakerController=sonybravia.SpeakerController(device=device)
                        # On the XBR-75X850C that this was built for, there are only two actual supported modes: audioSystem and speaker
                        # and they are reversed!!  speaker will send audio to the receiver and audioSystem is the in-TV speaker

                        # update 8/6/20 - tv firmware Oreo 8.0 may have now fixed it - swapping it back
                        #device.AudioModeController=sonybravia.AudioModeController('Audio', device=device, 
                        #    supportedModes={'audioSystem': 'Receiver', "speaker": 'TV', "speaker_hdmi":'Both', "hdmi":'HDMI'})
                        device.AudioModeController=sonybravia.AudioModeController('Audio', device=device, 
                            supportedModes={'speaker': 'TV', "audioSystem": 'Receiver'})
                        device.PowerSavingModeController=sonybravia.PowerSavingModeController('PowerSaving', device=device, 
                            supportedModes={'off': 'Off', "low": "Low", "high": "High", "pictureOff": "Picture Off"})

                        # TV is plugged into sound system so skipping speaker here, but could be added
                        return self.dataset.newaddDevice(device)
                return False
            except:
                self.log.error('!! Error adding smart TV', exc_info=True)
            
        def findRemoteCode(self, codename):

            try:
                for code in self.dataset.nativeDevices['tv']['BRAVIA']['remoteCommands']:
                    if code['name']==codename:
                        #self.log.info('Found code for %s: %s' % (codename, code['value']))
                        return code['value']
                self.log.info('No code found for %s' % codename)
                return ''
            except:
                self.log.error('Error getting remote code', exc_info=True)
                return ''

        def getDetailsFromURI(self, uri):
            
            try:
                result={}
                conninfo=uri.split('?')[0]
                result['source']=conninfo.split(':')[0]
                result['type']=conninfo.split(':')[1]
                
                details=uri.split('?')[1]
                details=details.split('&')
                for detail in details:
                    dparts=detail.split('=')
                    result[dparts[0]]=dparts[1]
                    
                return result
            except:
                self.log.error('Error parsing input URI: %s' % uri, exc_info=True)
                

        def parse_input_name(self,nativeObj):
            
            try:
                if 'PlayingContentInfo' not in nativeObj:
                    #self.log.warn('No playing content')
                    return 'Android TV'

                if 'uri' in nativeObj['PlayingContentInfo']:
                    details=self.getDetailsFromURI(nativeObj['PlayingContentInfo']['uri'])
                    if nativeObj['PlayingContentInfo']['uri'].startswith('extInput:cec') or details['type'] in ['cec','hdmi','player']:
                        if details['port'] in self.config.hdmi_port_names:
                            return self.config.hdmi_port_names[details['port']]
                if 'title' in nativeObj['PlayingContentInfo']:
                    return nativeObj['PlayingContentInfo']['title']
                else:
                    return 'Android TV'
                    
            except:
                self.log.error('Error getting virtual input name for %s' % nativeObj['PlayingContentInfo'], exc_info=True)


if __name__ == '__main__':
    adapter=sonybravia(name="sonybravia")
    adapter.start()