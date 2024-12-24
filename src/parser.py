#!/usr/bin/env python3

import rospy
from std_msgs.msg import Float32, String
import socket
from pyais import decode
from pyais.exceptions import InvalidNMEAMessageException
import time


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


def nmea_parser():
    rospy.init_node('udp_listener', anonymous=True)

    # Publishers for Furuno SCX20 data
    message_pub = rospy.Publisher('message', String, queue_size=10)
    heading_pub = rospy.Publisher('heading', Float32, queue_size=10)
    pitch_pub = rospy.Publisher('pitch', Float32, queue_size=10)
    yaw_pub = rospy.Publisher('yaw', Float32, queue_size=10)
    roll_pub = rospy.Publisher('roll', Float32, queue_size=10)
    latitude_pub = rospy.Publisher('latitude', Float32, queue_size=10)
    longitude_pub = rospy.Publisher('longitude', Float32, queue_size=10)
    speedkmh_pub = rospy.Publisher('speedkmh', Float32, queue_size=10)
    speedknots_pub = rospy.Publisher('speedknots', Float32, queue_size=10)
    truecourse_pub = rospy.Publisher('truecourse', Float32, queue_size=10)
    heave_pub = rospy.Publisher('heave', Float32, queue_size=10)
    AIS_pub = rospy.Publisher('aivdm', String, queue_size=10)
    AIS_array_pub = rospy.Publisher('aisarray', String, queue_size=10)
    rate = rospy.Rate(50)  # 50hz

    # Set up the UDP socket for both Furuno SCX20 and em-trak B921 AIS
    UDP_IP = "0.0.0.0"  # Listen on all interfaces
    UDP_PORT = 10110
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((UDP_IP, UDP_PORT))

    # rospy.loginfo("UDP Listener started on port %d", UDP_PORT)

    while not rospy.is_shutdown():
        try:
            # Read data from UDP socket
            data, addr = sock.recvfrom(1024)  # buffer size is 1024 bytes
            message = data.decode('utf-8').strip()
            message_pub.publish(message)

            # rospy.loginfo("Received packet from %s:%d", addr[0], addr[1])
            # rospy.loginfo("Packet data: %s", message)
            # Parse and publish specific data based on NMEA sentence type

            # HEADING
            if message.startswith('$HEHDT'):
                parts = message.split(',')
                heading = float(parts[1])
                heading_pub.publish(heading)
                # rospy.loginfo("Heading %f", heading)

            # ROLL,PITCH,YAW
            elif message.startswith('$YXXDR'):
                parts = message.split(',')

                if parts[1] == 'A':
                    yaw = float(parts[2])
                    pitch = float(parts[6])
                    roll = float(parts[10])

                    yaw_pub.publish(yaw)
                    pitch_pub.publish(pitch)
                    roll_pub.publish(roll)
                    # rospy.loginfo("Attitude Roll:%f, Pitch:%f, Yaw:%f", roll,pitch,yaw)
                elif parts[1] == 'D':
                    heave = float(parts[2])
                    heave_pub.publish(heave)
                    # rospy.loginfo("Heave:%f", heave)

            # LATITUDE, LONGITUDE
            elif message.startswith('$GPGGA'):
                parts = message.split(',')

                # Extract latitude and longitude strings
                latitude_str = parts[2]
                latitude_direction = parts[3]
                longitude_str = parts[4]
                longitude_direction = parts[5]

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
                parts = message.split(',')

                # Extract the true course and speeds
                true_course = float(parts[1])
                ground_speed_knots = float(parts[5])
                ground_speed_kmh = float(parts[7])
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
        nmea_parser()
    except rospy.ROSInterruptException:
        pass
