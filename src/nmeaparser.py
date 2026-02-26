#!/usr/bin/env python3

"""
ROS Package for NMEA Parsing
------------------------------

Author:         Kaarel Koppel
Modified by:    Dhanushka Liyanage


Forked out from Kaarel's github.

Additional packages needed:
* geodesy
"""

import rospy
from std_msgs.msg import Float32, String
from geographic_msgs.msg import GeoPoint
import socket
import struct
import time
import math
from threading import Thread, Lock
from pyais import decode
from pyais.exceptions import InvalidNMEAMessageException

class MessageCache:
    def __init__(self):
        message_cache = dict()
    
    def put(self, message):
        header = ''
        data = ''



# from AIVDM_decoder import process_ais_message
class ShipInfo:
    def __init__(self, mmsi, position=None, heading=None, speed=None, timestamp=None, shipname=None):
        self.mmsi = mmsi
        self.position = position
        self.heading = heading
        self.speed = speed
        self.timestamp = timestamp
        self.shipname = shipname

    def update(self, position=None, heading=None, speed=None, timestamp=None, shipname=None):
        if position is not None:
            self.position = position
        if heading is not None:
            self.heading = heading
        if speed is not None:
            self.speed = speed
        if timestamp is not None:
            self.timestamp = timestamp
        if shipname is not None:
            self.shipname = shipname


# Dictionary to store ships by their MMSI
ships = {}
# Temporary storage for multi-part messages with their timestamps
multi_part_messages = {}


def process_ais_message(message):
    global ships, multi_part_messages

    try:
        # Ensure the message is in bytes
        if isinstance(message, str):
            message = message.encode('utf-8')

        # Split the message into parts
        message_parts = message.decode('utf-8').split(',')

        if message_parts[0] != "!AIVDM":
            return None  # Not an AIVDM message

        total_parts = int(message_parts[1])
        part_number = int(message_parts[2])
        sequence_id = message_parts[3]
        message_content = message_parts[5]

        # Initialize the sequence ID if not present
        if sequence_id not in multi_part_messages:
            multi_part_messages[sequence_id] = {'parts': [None] * total_parts, 'timestamp': time.time()}

        # Store the part
        multi_part_messages[sequence_id]['parts'][part_number - 1] = message_content

        # Update the timestamp
        multi_part_messages[sequence_id]['timestamp'] = time.time()

        # Check if we have received all parts
        if None not in multi_part_messages[sequence_id]['parts']:
            full_message = "!AIVDM,1,1,,A," + "".join(multi_part_messages[sequence_id]['parts']) + ",0*00"
            del multi_part_messages[sequence_id]
            decoded_message = decode(full_message.encode('utf-8'))
        else:
            return None  # Wait for more parts to complete the message

        # Extract required information using attributes instead of subscripts
        mmsi = decoded_message.mmsi
        position = (getattr(decoded_message, 'lat', None), getattr(decoded_message, 'lon', None))
        heading = getattr(decoded_message, 'heading', None)
        if heading == 511:
            heading = None
        speed = getattr(decoded_message, 'sog', None)
        shipname = getattr(decoded_message, 'shipname', None)
        timestamp = time.time()

        # Update the ship information in the dictionary
        if mmsi in ships:
            ships[mmsi].update(position, heading, speed, timestamp, shipname)
        else:
            ships[mmsi] = ShipInfo(mmsi, position, heading, speed, timestamp, shipname)

        # Clean up old entries and incomplete messages
        current_time = time.time()
        for mmsi in list(ships.keys()):
            if current_time - ships[mmsi].timestamp > 600:
                del ships[mmsi]

        for seq_id in list(multi_part_messages.keys()):
            if current_time - multi_part_messages[seq_id]['timestamp'] > 600:
                del multi_part_messages[seq_id]

        return ships

    except InvalidNMEAMessageException as e:
        print(f"Invalid NMEA message: {message}")
    except Exception as e:
        print(f"Error processing message: {e}")

    return None


def format_ships_data(ships):
    formatted_data = ""
    for ship in ships.values():
        formatted_data += f"MMSI: {ship.mmsi}, Name: {ship.shipname}, Pos: {ship.position}, Heading: {ship.heading}, Speed: {ship.speed}" + '\n'
    return formatted_data

class NMEA_Listener:
    def __init__(self):

        # self.message_buffer = message_buffer
        # self.lock = lock

        rospy.init_node('udp_listener', anonymous=True)

        # Set up the UDP socket for both Furuno SCX20 and em-trak B921 AIS
        UDP_IP = "0.0.0.0"  # Listen on all interfaces
        UDP_PORT = 10110
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((UDP_IP, UDP_PORT))

        rospy.loginfo("UDP Listener started on port %d", UDP_PORT)

    def get_checksum(self, payload_string) -> int:

        payload_hex_lst = list(payload_string)
        cumulative_xor = 0

        for i in range(len(payload_hex_lst)):

            int_from_ascii_char = ord(payload_hex_lst[i])
            cumulative_xor ^= int_from_ascii_char

        check_sum = hex(cumulative_xor)
        return check_sum

    def to_signed(self, value):

        if value == 0x7FFF:
            return 0
        signed_value = value & 0xFFFF

        if signed_value & 0x8000:
            signed_value -= 0x10000

        return signed_value

    def nmea_parser(self):
        # Publishers for Furuno SCX20 data
        message_pub = rospy.Publisher('message', String, queue_size=10)
        heading_pub = rospy.Publisher('heading', Float32, queue_size=10)
        pitch_pub = rospy.Publisher('pitch', Float32, queue_size=10)
        yaw_pub = rospy.Publisher('yaw', Float32, queue_size=10)
        roll_pub = rospy.Publisher('roll', Float32, queue_size=10)
        latitude_pub = rospy.Publisher('latitude', Float32, queue_size=10)
        longitude_pub = rospy.Publisher('longitude', Float32, queue_size=10)
        geo_position_pub = rospy.Publisher('geoposition', GeoPoint, queue_size=10)
        speedkmh_pub = rospy.Publisher('speedkmh', Float32, queue_size=10)
        speedknots_pub = rospy.Publisher('speedknots', Float32, queue_size=10)
        truecourse_pub = rospy.Publisher('truecourse', Float32, queue_size=10)
        heave_pub = rospy.Publisher('heave', Float32, queue_size=10)
        AIS_pub = rospy.Publisher('aivdm', String, queue_size=10)
        AIS_array_pub = rospy.Publisher('aisarray', String, queue_size=10)

        rate = rospy.Rate(50)  # 50hz

        while not rospy.is_shutdown():
            try:
                # Read data from UDP socket
                data, addr = self.sock.recvfrom(1024)  # buffer size is 1024 bytes
                message = data.decode('utf-8').strip()
                message_pub.publish(message)

                rospy.loginfo("Received packet from %s:%d", addr[0], addr[1])
                rospy.loginfo("Packet data: %s", message)

                field_chars = list(message)
                field_string = ''

                for i in range(1, len(field_chars) - 3):
                    field_string += field_chars[i]

                check_sum = self.get_checksum(field_string)


                # Decoding NMEA 2000 messages
                if message.startswith('$MXPGN'):
                    nmea_split_strings = message.split(',')
                    
                    # Print the message content on terminal.
                    rospy.loginfo(f"NMEA message components: {nmea_split_strings[0]} ; {nmea_split_strings[1]} ; {nmea_split_strings[2]} ; {nmea_split_strings[3]} : with number of parts = {len(nmea_split_strings)}")
                    nmea_pgn_id = nmea_split_strings[1]

                    # 1. PGN 127250 Heading
                    if nmea_pgn_id == '01F112':
                        rospy.loginfo(f'PGN127250 - Heading: {nmea_split_strings[3]}')

                        pgn_fields = list(nmea_split_strings[3])

                        seq_id = int('0x' + pgn_fields[0] + pgn_fields[1], 16)

                        # Compass heading
                        heading_raw = int('0x' + pgn_fields[2] + pgn_fields[3] + pgn_fields[4] + pgn_fields[5], 16)
                        heading_radians = heading_raw * 0.001
                        heading_degrees = (heading_radians * 360) / (2 * math.pi)

                        rospy.loginfo(f'Heading: {heading_degrees} deg')

                        # Publish heading value to the heading topic
                        heading_pub.publish(heading_degrees)

                        # Heading deviation
                        heading_deviation = int('0x' + pgn_fields[6] + pgn_fields[7], 16)

                        # Heading variation
                        heading_variation = int('0x' + pgn_fields[10] + pgn_fields[11], 16)

                    # 2. PGN 127252 Heave
                    elif nmea_pgn_id == '01F114':
                        rospy.loginfo(f'PGN127252 - Heave: {nmea_split_strings[3]}')

                        pgn_fields = list(nmea_split_strings[3])

                        seq_id = int('0x' + pgn_fields[0] + pgn_fields[1], 16)

                        # Heave
                        heave_raw = int('0x' + pgn_fields[2] + pgn_fields[3] + pgn_fields[4] + pgn_fields[5], 16)
                        heave_meters = heave_raw * 0.01
                        
                        rospy.loginfo(f'Heave: {heave_meters} m')
                    

                    # 3. PGN 127257 Attitude
                    elif nmea_pgn_id == '01F119':
                        rospy.loginfo(f'PGN127257 - Attitude: {nmea_split_strings[3]} ')

                        pgn_fields = list(nmea_split_strings[3])

                        seq_id = int('0x' + pgn_fields[0] + pgn_fields[1], 16)

                        # Roll
                        roll_raw = int('0x' + pgn_fields[2] + pgn_fields[3] + pgn_fields[4] + pgn_fields[5], 16)
                        roll_radians = self.to_signed(roll_raw) * 0.0001
                        roll_degrees = (roll_radians * 180) / math.pi

                        # Pitch
                        pitch_raw = int('0x' + pgn_fields[6] + pgn_fields[7]+ pgn_fields[8] + pgn_fields[9], 16)
                        pitch_radians = self.to_signed(pitch_raw) * 0.0001
                        pitch_degrees = (pitch_radians * 180) / math.pi

                        # Yaw
                        yaw_raw = int('0x' + pgn_fields[10] + pgn_fields[11] + pgn_fields[12] + pgn_fields[13], 16)
                        yaw_radians = self.to_signed(yaw_raw) * 0.0001
                        yaw_degrees = (yaw_radians * 180) / math.pi

                        rospy.loginfo(f'Yaw: {yaw_degrees} deg, pitch: {pitch_degrees} deg, roll: {roll_degrees} deg')

                    # 4. PGN 129025 Position rapid update
                    elif nmea_pgn_id == '01F801':
                        rospy.loginfo(f'PGN129025 - Position rapid update: {nmea_split_strings[3]}')

                        pgn_fields = list(nmea_split_strings[3])

                        latitude_raw = int('0x' + pgn_fields[0] + pgn_fields[1] + pgn_fields[2] + pgn_fields[3] + pgn_fields[4] + pgn_fields[5] + pgn_fields[6] + pgn_fields[7], 16)
                        latitude_degrees = latitude_raw * 1e-07

                        longitude_raw = int('0x' + pgn_fields[8] + pgn_fields[9] + pgn_fields[10] + pgn_fields[11] + pgn_fields[12] + pgn_fields[13] + pgn_fields[14] + pgn_fields[15], 16)
                        longitude_degrees = longitude_raw * 1e-07

                        latitude_pub.publish(latitude_degrees)
                        longitude_pub.publish(longitude_degrees)

                        rospy.loginfo(f'Latitude: {latitude_degrees}, Longitude: {longitude_degrees}')

                    # 5. PGN 130578 Vessel speed components
                    elif nmea_pgn_id == '01FE12':
                        rospy.loginfo(f'PGN130578 - Vessel speed components ToDo: {nmea_split_strings[3]}')

                    # 6. PGN 127251 Rate of turn
                    elif nmea_pgn_id == '01F113':
                        rospy.loginfo(f'PGN127251 - Rate of turn: {nmea_split_strings[3]}')

                        pgn_fields = list(nmea_split_strings[3])

                        rate_of_turn_raw = int('0x' + pgn_fields[2] + pgn_fields[3] + pgn_fields[4] + pgn_fields[5] + pgn_fields[6] + pgn_fields[7] + pgn_fields[8] + pgn_fields[9], 16)
                        rate_of_turn_radians = rate_of_turn_raw * 3.125e-08
                        rate_of_turn_degrees = (rate_of_turn_radians * 180) / math.pi

                        if rate_of_turn_degrees > 360:
                            rospy.loginfo(f'Rate of turn not provided by sensors.')
                        else:
                            rospy.loginfo(f'Rate of turn: {rate_of_turn_degrees}, degree/sec')

                    # 7. PGN 129026 COG and SOG
                    elif nmea_pgn_id == '01F802':
                        rospy.loginfo(f'PGN129026 - COG and SOG: {nmea_split_strings[3]}')

                        pgn_fields = list(nmea_split_strings[3])

                        cog_raw = int('0x' + pgn_fields[4] + pgn_fields[5] + pgn_fields[6] + pgn_fields[7], 16)
                        cog_radians = cog_raw * 0.001
                        cog_degrees = (cog_radians * 180) / math.pi

                        sog_raw = int('0x' + pgn_fields[8] + pgn_fields[9] + pgn_fields[10] + pgn_fields[11], 16)
                        sog_meters_per_sec = sog_raw * 0.01

                        rospy.loginfo(f'COG: {cog_degrees} degrees and SOG {sog_meters_per_sec}')

                # HEADING
                if message.startswith('$HEHDT'):
                    nmea_split_strings = message.split(',')
                    heading = float(nmea_split_strings[1])
                    heading_pub.publish(heading)
                    rospy.loginfo("Heading %f", heading)

                # ROLL,PITCH,YAW
                elif message.startswith('$YXXDR'):
                    nmea_split_strings = message.split(',')

                    message_string_parts_count = len(nmea_split_strings)
                    rospy.loginfo("No of parts: %d", message_string_parts_count)

                    if nmea_split_strings[1] == 'D' and message_string_parts_count == 5:
                        heave = float(nmea_split_strings[2])
                        heave_pub.publish(heave)
                        
                        rospy.loginfo("Heave:%f", heave)

                    if nmea_split_strings[1] == 'A' and message_string_parts_count > 10:
                        rospy.loginfo("Trying to read a short message")

                        yaw = round(float(nmea_split_strings[2]), 1)
                        pitch = round(float(nmea_split_strings[6]), 1)
                        roll = round(float(nmea_split_strings[10]), 1)

                        yaw_pub.publish(yaw)
                        pitch_pub.publish(pitch)
                        roll_pub.publish(roll)
                        
                        rospy.loginfo("Attitude Roll:%.1f, Pitch:%f, Yaw:%.1f", roll,pitch,yaw)

                    elif nmea_split_strings[1] == 'A' and message_string_parts_count == 9:

                        pitch = round(float(nmea_split_strings[2]), 1)
                        roll = round(float(nmea_split_strings[6]),1)

                        pitch_pub.publish(pitch)
                        roll_pub.publish(roll)
                        
                        rospy.loginfo("Attitude Roll:%.1f, Pitch:%f", roll, pitch)

                # WIND TEMPERATURE
                elif message.startswith('$WIXDR'):
                    nmea_split_strings = message.split(',')

                    message_string_parts_count = len(nmea_split_strings)

                    if message_string_parts_count < 5:
                        continue

                    if nmea_split_strings[1] == 'C':
                        wind_temperature = float(nmea_split_strings[2])
                        rospy.loginfo("Wind temperature: %.1f", wind_temperature)

                # LATITUDE, LONGITUDE
                # Minimum GNSS sentence
                elif message.startswith('$GPRMC'):
                    nmea_split_strings = message.split(',')

                    # Check if it is a valid message
                    if nmea_split_strings[2] == 'A':
                        # Data valid

                        # Extract latitude and longitude strings
                        latitude_str = nmea_split_strings[3]
                        latitude_direction = nmea_split_strings[4]
                        longitude_str = nmea_split_strings[5]
                        longitude_direction = nmea_split_strings[6]

                        # Convert latitude and longitude to float
                        # Latitude: DDMM.MMMMM -> DD + MM.MMMMM / 60
                        latitude_deg = int(latitude_str[:2])
                        latitude_min = float(latitude_str[2:])
                        latitude = latitude_deg + (latitude_min / 60.0)

                        # If the latitude direction is South, make the latitude negative
                        if latitude_direction == 'S':
                            latitude = -latitude

                        # Longitude: DDDMM.MMMMM -> DDD + MM.MMMMM / 60
                        longitude_deg = int(longitude_str[:3])
                        longitude_min = float(longitude_str[3:])
                        longitude = longitude_deg + (longitude_min / 60.0)

                        # If the longitude direction is West, make the longitude negative
                        if longitude_direction == 'W':
                            longitude = -longitude

                        latitude_pub.publish(latitude)
                        longitude_pub.publish(longitude)

                        current_geo_point = GeoPoint(latitude, longitude, 0)
                        geo_position_pub.publish(current_geo_point)

                        rospy.loginfo("Latitude:%f, Longitude:%f", latitude, longitude)

                    elif nmea_split_strings[2] == 'V':
                        # Data invalid
                        rospy.logerr("GNSS data invalid.")
                        continue


                elif message.startswith('$GPGGA'):
                    nmea_split_strings = message.split(',')

                    # Extract latitude and longitude strings
                    latitude_str = nmea_split_strings[2]
                    latitude_direction = nmea_split_strings[3]
                    longitude_str = nmea_split_strings[4]
                    longitude_direction = nmea_split_strings[5]

                    # Convert latitude and longitude to float
                    # Latitude: DDMM.MMMMM -> DD + MM.MMMMM / 60
                    latitude_deg = int(latitude_str[:2])
                    latitude_min = float(latitude_str[2:])
                    latitude = latitude_deg + (latitude_min / 60.0)

                    # If the latitude direction is South, make the latitude negative
                    if latitude_direction == 'S':
                        latitude = -latitude

                    # Longitude: DDDMM.MMMMM -> DDD + MM.MMMMM / 60
                    longitude_deg = int(longitude_str[:3])
                    longitude_min = float(longitude_str[3:])
                    longitude = longitude_deg + (longitude_min / 60.0)

                    # If the longitude direction is West, make the longitude negative
                    if longitude_direction == 'W':
                        longitude = -longitude

                    latitude_pub.publish(latitude)
                    longitude_pub.publish(longitude)
                    # rospy.loginfo("Latitude:%f, Longitude:%f", latitude, longitude)


                # TRUE COURSE, SPEED IN KMH AND KNOTS
                elif message.startswith('$GPVTG'):
                    nmea_split_strings = message.split(',')

                    # Extract the true course and speeds
                    true_course = float(nmea_split_strings[1])
                    ground_speed_knots = float(nmea_split_strings[5])
                    ground_speed_kmh = float(nmea_split_strings[7])
                    speedkmh_pub.publish(ground_speed_kmh)
                    speedknots_pub.publish(ground_speed_knots)
                    truecourse_pub.publish(true_course)
                    # rospy.loginfo("Speed (knots):%f, True course:%f", ground_speed_knots, true_course)

                # AIVDM decoding
                elif message.startswith('!AIVDM'):
                    rospy.loginfo("AIVDM processed")
                    AIS_pub.publish(message)
                    #  process_ais_message(message)
                    ships = process_ais_message(message)
                    if ships:
                        formatted_data = format_ships_data(ships)
                        AIS_array_pub.publish(formatted_data)


            except Exception as e:
                rospy.logerr("Error: %s", str(e))

            rate.sleep()


if __name__ == '__main__':
    try:
        nmear_parser_obj = NMEA_Listener()

        nmear_parser_obj.nmea_parser()

    except rospy.ROSInterruptException:
        pass
