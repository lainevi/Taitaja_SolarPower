# Thingspeak related imports
from __future__ import print_function
import paho.mqtt.publish as publish
import psutil

# Aurora client related imports
import time, datetime
from aurorapy.client import AuroraError, AuroraSerialClient

clients=[1,2,3,4]
#client = AuroraSerialClient(address=2, port='/dev/ttyS0', baudrate=19200, parity='N', stop_bits=1, data_bits=8, timeout=5)
clients[0] = AuroraSerialClient(port='/dev/ttyAMA0', address=2, baudrate=19200, parity='N', stop_bits=1, timeout=3)
clients[1] = AuroraSerialClient(port='/dev/ttyAMA0', address=3, baudrate=19200, parity='N', stop_bits=1, timeout=3)
clients[2] = AuroraSerialClient(port='/dev/ttyAMA0', address=4, baudrate=19200, parity='N', stop_bits=1, timeout=3)

# Datestamp value received from ABB inverters starts from midnight of January 1, 2000
# So we need to add this to the received timestamp
# returns seconds from epoch to January 1, 2000
start_date = datetime.datetime(2000,1,1,0,0,0).timestamp()

try:
    # Next cycle start time (Start immediately)
    next_time = time.time()
    # Which client to start
    client_turn = 0
    # Poll interval between clients
    cycle_interval = 15 # seconds
    
    # Get measurements
    # Thingspeak channel handles 8 fields
    # -1 is error value and will not be sended. On error function will return either -1 or 'N/A'
    fields=[[-1,-1,-1,-1,-1,-1,-1,-1],[-1,-1,-1,-1,-1,-1,-1,-1],[-1,-1,-1,-1,-1,-1,-1,-1],[-1,-1,-1,-1,-1,-1,-1,-1]]
    # Also store status values to list
    states = [[-1,-1,-1,-1,-1],[-1,-1,-1,-1,-1],[-1,-1,-1,-1,-1],[-1,-1,-1,-1,-1]]
    
    # Run forever
    while(1):
        
        # Cycle interval passed?
        if time.time() >= next_time:
            # Start next cycle after specified interval
            next_time += cycle_interval
            
            # Store last data
            last_fields = fields[client_turn]
            state_idx = last_fields[0:3]
            
            # Reset fields
            fields[client_turn] = [-1,-1,-1,-1,-1,-1,-1,-1]
            states[client_turn] = [-1,-1,-1,-1,-1]

            # "Client" 3 is used to calculate sums of all inverters. Clients 0, 1 ,2 are the real inverters
            if client_turn == 3:
                print("\nCalculating totals from all clients")
                
                # Create status fields to send for inverters
                for client_nro in range(0,3):
                    # Client is operating in normal mode
                    if states[client_nro][0] == "Run" and \
                       states[client_nro][1] == "Run" and \
                       (states[client_nro][2] == "MPPT" or \
                       states[client_nro][3] == "MPPT") and \
                       states[client_nro][4] == "No alarm":
                        
                        fields[client_turn][client_nro] = 1
                    
                    # Can't receive any one of the state values
                    elif states[client_nro][0] in ["N/A", -1] or\
                         states[client_nro][1] in ["N/A", -1]  or \
                         states[client_nro][2] in ["N/A", -1]  or \
                         states[client_nro][3] in ["N/A", -1]  or \
                         states[client_nro][4] in ["N/A", -1]:
                        
                        fields[client_turn][client_nro] = state_idx[client_nro] - 1
                        # On third error pass change client status to 0
                        if fields[client_turn][client_nro] < -4:
                            fields[client_turn][client_nro] = 0
                        
                    else:
                        fields[client_turn][client_nro] = 0

                # Sum up values if all clients are operational (Not one of them is at error state -1(or more negative))
                if fields[client_turn][0] >= 0 and fields[client_turn][1] >= 0 and fields[client_turn][2] >= 0:
                    
                    for n in range(4,8):
                        # Sum up Energy values if all of them are available
                        if fields[0][n] != -1 and fields[1][n] != -1 and fields[2][n] != -1:
                            fields[client_turn][n] = round(fields[0][n] + fields[1][n] + fields[2][n],3)
                        
                        # On third error pass reset value to 0
                        else:
                            fields[client_turn][n] = 0
                            
                    # Sum up current power produced if all of them are available
                    if fields[0][3] != -1 and fields[1][3] != -1 and fields[2][3] != -1:
                        fields[client_turn][3] = round(fields[0][2] + fields[1][2] + fields[2][2],3)
                    
                    # On third error pass reset value to 0
                    else:
                        fields[client_turn][3] = 0


            # Clients 0(ID2),1(ID3),2(ID4) "real" RS-485 Aurora ABB Inverters
            else:
                #######
                #
                # Inverter code
                #
                ######
                
                # Assign correct client to measure
                client = clients[client_turn]
                
                # Open client connection
                client.connect()
                
                # Try to get the clients current timestamp
                client_time = client.time_date()
                
                # Only try to measure rest if timestamp is correctly received
                if client_time != -1:
                    print("\nClient:", client.address, datetime.datetime.fromtimestamp(client_time + start_date))
                    states[client_turn] = client.state(1)
                    print("Global state: %s, Inverter state: %s" % (states[client_turn][0],states[client_turn][1]))
                    print("DC/DC Channel 1 state: %s, DC/DC Channel 2 state: %s" % (states[client_turn][2],states[client_turn][3]))
                    print("Alarm state: %s" % (states[client_turn][4]))
                    
                    # Empties the alarm queue
                    # print("Last 4 alarms:", client.alarms())
                
                    # Measure only if inverter is at running state
                    #if states[0] == "Run" and states[1] == "Run":                
                    #print("Temperature: %.2f \u00b0C" % (client.measure(21)))
                    #print("Input 1: %.2f V" % client.measure(23))
                    #print("Input 1: %.2f A" % client.measure(25))
                    #print("Input 2: %.2f V" % client.measure(26))
                    #print("Input 2: %.2f A" % client.measure(27))
                    fields[client_turn][0] = grid_voltage = round(client.measure(1),4)
                    print("Grid Voltage: %.2f V" % grid_voltage)
                    fields[client_turn][1] = grid_current = round(client.measure(2),4)
                    print("Grid Current: %.2f A" % grid_current)
                    fields[client_turn][2] = grid_power = round(client.measure(3),4)
                    print("Grid Power: %.2f W" % grid_power)
                    fields[client_turn][3] = grid_frequency = round(client.measure(4),4)
                    print("Grid Frequency: %.2f Hz" % grid_frequency)
                    fields[client_turn][4] = cum_energy_day = round((client.cumulated_energy(0)),4)
                    print("Day %.2f kWh" % (cum_energy_day/1000))
                    fields[client_turn][5] = cum_energy_week = round((client.cumulated_energy(1)),4)
                    print("Week %.2f kWh" % (cum_energy_week/1000))
                    fields[client_turn][6] = cum_energy_month = round((client.cumulated_energy(3)),4)
                    print("Month %.2f kWh" % (cum_energy_month/1000))
                    fields[client_turn][7] = cum_energy_year = round((client.cumulated_energy(4)),4)
                    print("Year %.2f kWh" % (cum_energy_year/1000))
                    #print("Total %.2f kWh" % (client.cumulated_energy(5)/1000))
                    #print("Ppk Max: %.2f kW" % (client.measure(34)/1000))
                    #print("Ppk Today: %.2f kW" % (client.measure(35)/1000), "\n\n")
                else:
                    print("Client", client_turn + 2, "not online")
                # Close client connection
                client.close()
            
            
            
            #######
            #
            # Thingspeak connection
            #
            ######
            
            # The ThingSpeak Channel IDs
            # Replace this with your Channel IDs
            channelIDs = ["","","",""]
            channelID = channelIDs[client_turn]

            # The Write API Keys for the channel
            # Replace this with your Write API keys
            apiKeys = ["","","",""]
            apiKey = apiKeys[client_turn]
            
            # The Hostname of the ThinSpeak MQTT service
            mqttHost = "mqtt.thingspeak.com"

            # Set up the connection parameters based on the connection type
            tTransport = "tcp"
            tPort = 1883
            tTLS = None
            
            # Create the topic string
            topic = "channels/" + channelID + "/publish/" + apiKey
            
            # Create the payload string
            """
            tPayload = "field1=" + str(grid_voltage) + \
                       "&field2=" + str(grid_current) + \
                       "&field3=" + str(grid_power) + \
                       "&field4=" + str(grid_frequency) + \
                       "&field5=" + str(cum_energy_day) + \
                       "&field6=" + str(cum_energy_week) + \
                       "&field7=" + str(cum_energy_month) + \
                       "&field8=" + str(cum_energy_year)
            """
            tPayload = ""
            for n in range(8):
                if fields[client_turn][n] > -1:#!= -1:
                    if n>0:
                        tPayload += "&"
                    
                    tPayload += "field" + str(n+1) + "="
                    # Change Energy values from Wh to kWh
                    if n < 4:
                        tPayload += str(fields[client_turn][n])
                    else:
                        tPayload += str(fields[client_turn][n]/1000)
            
            
            #print(tPayload, len(tPayload))
            
            # attempt to publish this data to the topic
            # Only if there is something to send
            if len(tPayload) > 0:
                try:
                    print("Client:", client_turn + 2, "Sending data", tPayload)
                    print("To channel:", channelIDs[client_turn], "With Key:",apiKeys[client_turn])
                    publish.single(topic, payload=tPayload, hostname=mqttHost, port=tPort, tls=tTLS, transport=tTransport)
                except:
                    print ("There was an error while publishing the data.")
            else:
                    print("Nothing to send!\n")


            # Measure next client in next cycle or loop to first one
            client_turn += 1
            if client_turn == len(clients):
                client_turn = 0

        else:
            # Sleep if there is still time left for next cycle
            #print("Client %d measurement cycle in: %d s" % (clients[client_turn].address, round(next_time-time.time())))
            time.sleep(1)
    
except AuroraError as e:
    print(str(e))
finally:
    print("Finally")
    # Close RS-485 ports
    # skip the last one, because it is not real RS485 client
    for n in range(len(clients)-1):
        client.close()

