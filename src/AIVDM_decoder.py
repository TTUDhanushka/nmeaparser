from pyais import decode
from pyais.exceptions import InvalidNMEAMessageException
import time


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
