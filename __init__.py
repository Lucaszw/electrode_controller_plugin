"""
Copyright 2015 Christian Fobel

This file is part of electrode_controller_plugin.

electrode_controller_plugin is free software: you can redistribute it and/or
modify it under the terms of the GNU General Public License as published by the
Free Software Foundation, either version 3 of the License, or (at your option)
any later version.

electrode_controller_plugin is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for
more details.

You should have received a copy of the GNU General Public License
along with electrode_controller_plugin.  If not, see <http://www.gnu.org/licenses/>.
"""
import logging
import json

from zmq_plugin.plugin import Plugin as ZmqPlugin
from zmq_plugin.schema import decode_content_data, pandas_object_hook, PandasJsonEncoder
import gobject
import gtk
import pandas as pd
import paho_mqtt_helpers as pmh

import zmq

from ...app_context import get_app, get_hub_uri
from ...plugin_helpers import StepOptionsController
from ...plugin_manager import (PluginGlobals, SingletonPlugin, IPlugin,
                               implements, emit_signal)

logger = logging.getLogger(__name__)

def drop_duplicates_by_index(series):
    '''
    Drop all but first entry for each set of entries with the same index value.

    Args:

        series (pandas.Series) : Input series.

    Returns:

        (pandas.Series) : Input series with *first* value in `series` for each
            *distinct* index value (i.e., duplicate entries dropped for same
            index value).
    '''
    return series[~series.index.duplicated()]

PluginGlobals.push_env('microdrop')


class ElectrodeControllerPlugin(SingletonPlugin, StepOptionsController,
                                pmh.BaseMqttReactor):
    """
    This class is automatically registered with the PluginManager.
    """
    implements(IPlugin)
    plugin_name = 'microdrop.electrode_controller_plugin'

    def __init__(self):
        self.name = self.plugin_name
        # self.plugin = None
        self.command_timeout_id = None
        pmh.BaseMqttReactor.__init__(self)
        self.start()

    @property
    def electrode_states(self):
        # Set the state of DMF device channels.
        step_options = self.get_step_options()
        return step_options.get('electrode_states', pd.Series())

    @electrode_states.setter
    def electrode_states(self, electrode_states):
        # Set the state of DMF device channels.
        step_options = self.get_step_options()
        step_options['electrode_states'] = electrode_states

    def on_connect(self, client, userdata, flags, rc):
        self.mqtt_client.subscribe('microdrop/droplet-planning-plugin/set-electrode-states')
        self.mqtt_client.subscribe('microdrop/dmf-device-ui/set-electrode-states')
        # TODO: Possibly depricate set-electrode-state
        self.mqtt_client.subscribe('microdrop/droplet-planning-plugin/set-electrode-state')
        self.mqtt_client.subscribe('microdrop/dmf-device-ui/set-electrode-state')
        self.mqtt_client.subscribe('microdrop/dmf-device-ui/get-channel-states')

    def on_message(self, client, userdata, msg):
        '''
        Callback for when a ``PUBLISH`` message is received from the broker.
        '''
        logger.info('[on_message] %s: "%s"', msg.topic, msg.payload)

        if msg.topic == 'microdrop/droplet-planning-plugin/set-electrode-states':
            data = json.loads(msg.payload, object_hook=pandas_object_hook)
            self.set_electrode_states(data)

        if msg.topic == 'microdrop/dmf-device-ui/set-electrode-states':
            data = json.loads(msg.payload, object_hook=pandas_object_hook)
            self.set_electrode_states(data)

        if msg.topic == 'microdrop/dmf-device-ui/set-electrode-state':
            self.set_electrode_state(json.loads(msg.payload))

        if msg.topic == 'microdrop/dmf-device-ui/get-channel-states':
            self.get_channel_states()

    def on_plugin_enable(self):
        """
        Handler called once the plugin instance is enabled.

        Note: if you inherit your plugin from AppDataController and don't
        implement this handler, by default, it will automatically load all
        app options from the config file. If you decide to overide the
        default handler, you should call:

            AppDataController.on_plugin_enable(self)

        to retain this functionality.
        """
        pass

    def get_actuated_area(self, electrode_states):
        '''
        Get area of actuated electrodes.
        '''
        app = get_app()
        actuated_electrodes = electrode_states[electrode_states > 0].index
        return app.dmf_device.electrode_areas.ix[actuated_electrodes].sum()

    def get_channel_states(self):
        '''
        Returns:

            (pandas.Series) : State of channels, indexed by channel.
        '''
        result = self.get_state(self.electrode_states)
        result['actuated_area'] = self.get_actuated_area(result
                                                         ['electrode_states'])
        data = {}
        data['electrode_states'] = result['electrode_states']
        data['channel_states']   = result['channel_states']
        data['actuated_area']    = result['actuated_area']

        self.mqtt_client.publish('microdrop/electrode-controller-plugin/get-channel-states',
                                  json.dumps(data, cls=PandasJsonEncoder),
                                  retain=True)

        return result

    def get_state(self, electrode_states):
        app = get_app()
        electrode_channels = (app.dmf_device
                              .actuated_channels(electrode_states.index)
                              .dropna().astype(int))

        # Each channel should be represented *at most* once in
        # `channel_states`.
        channel_states = pd.Series(electrode_states
                                   .ix[electrode_channels.index].values,
                                   index=electrode_channels)
        # Duplicate entries may result from multiple electrodes mapped to the
        # same channel or vice versa.
        channel_states = drop_duplicates_by_index(channel_states)

        channel_electrodes = (app.dmf_device.electrodes_by_channel
                              .ix[channel_states.index])
        electrode_states = pd.Series(channel_states
                                     .ix[channel_electrodes.index].values,
                                     index=channel_electrodes.values)

        # Each electrode should be represented *at most* once in
        # `electrode_states`.
        return {'electrode_states': drop_duplicates_by_index(electrode_states),
                'channel_states': channel_states}

    def set_electrode_state(self, msg):
        '''
        Set the state of a single electrode.

        Args:

            electrode_id (str) : Electrode identifier (e.g., `"electrode001"`)
            state (int) : State of electrode
        '''
        data = {}
        data['electrode_states'] = pd.Series([msg['state']],
                                        index=[msg['electrode_id']])
        return self.set_electrode_states(data)

    def set_electrode_states(self, data):
        '''
        Set the state of multiple electrodes.

        Args:

            electrode_states (pandas.Series) : State of electrodes, indexed by
                electrode identifier (e.g., `"electrode001"`).
            save (bool) : Trigger save request for protocol step.

        Returns:

            (dict) : States of modified channels and electrodes, as well as the
                total area of all actuated electrodes.
        '''
        app = get_app()

        electrode_states = data['electrode_states']

        if 'save' in data:
            save = data['save']
        else:
            save = False

        result = self.get_state(electrode_states)

        # Set the state of DMF device channels.
        self.electrode_states = (result['electrode_states']
                                 .combine_first(self.electrode_states))

        if save:
            def notify(step_number):
                emit_signal('on_step_options_changed', [self.name,
                                                        step_number],
                            interface=IPlugin)
            gtk.idle_add(notify, app.protocol.current_step_number)

        result['actuated_area'] = self.get_actuated_area(self.electrode_states)

        data = {}
        data['electrode_states'] = self.electrode_states

        self.mqtt_client.publish('microdrop/electrode-controller-plugin/set-electrode-states',
                                  json.dumps(data,cls=PandasJsonEncoder),
                                  retain=True)

        return result

    def on_plugin_disable(self):
        """
        Handler called once the plugin instance is disabled.
        """
        pass
    def on_app_exit(self):
        """
        Handler called just before the MicroDrop application exits.
        """
        pass

    def on_step_swapped(self, old_step_number, step_number):
        self.get_channel_states()


PluginGlobals.pop_env()
