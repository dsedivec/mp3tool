import struct
import ctypes
import logging as _logging

from mp3tool import madctypes

__all__ = ["scan_mp3", "AudioFrame", "Tag", "UnknownData",
           "MODE_SINGLE_CHANNEL", "MODE_DUAL_CHANNEL",
           "MODE_JOINT_STEREO", "MODE_STEREO"]

MODE_SINGLE_CHANNEL = madctypes.MAD_MODE_SINGLE_CHANNEL
MODE_DUAL_CHANNEL = madctypes.MAD_MODE_DUAL_CHANNEL
MODE_JOINT_STEREO = madctypes.MAD_MODE_JOINT_STEREO
MODE_STEREO = madctypes.MAD_MODE_STEREO

logger = _logging.getLogger()

class Part (object):
    def __init__(self, start, end):
        self.start = start
        self.end = end

    def _repr_helper(self, other_attrs):
        attrs = ("start", "end") + other_attrs
        attr_strs = ["%s=%r" % (name, getattr(self, name)) for name in attrs]
        return "%s(%s)" % (self.__class__.__name__, ", ".join(attr_strs))

class VerifiablePart (Part):
    def __init__(self, error_messages, **kwargs):
        Part.__init__(self, **kwargs)
        self.error_messages = tuple(error_messages)
        self.valid = not error_messages

    def _repr_helper(self, other_attrs):
        return Part._repr_helper(self, ("error_messages",) + other_attrs)

class AudioFrame (VerifiablePart):
    def __init__(self, layer, bit_rate, mode, sampling_rate, **kwargs):
        VerifiablePart.__init__(self, **kwargs)
        self.layer = layer
        self.bit_rate = bit_rate
        self.mode = mode
        self.sampling_rate = sampling_rate

    def __repr__(self):
        return self._repr_helper(("layer", "bit_rate", "mode",
                                  "sampling_rate"))

class Tag (VerifiablePart):
    ID3V1 = "ID3v1"
    ID3V2 = "ID3v2"
    APEV2 = "APEv2"
    LYRICS3V2 = "Lyrics3v2"

    def __init__(self, tag_type, **kwargs):
        VerifiablePart.__init__(self, **kwargs)
        self.tag_type = tag_type

    def __repr__(self):
        return self._repr_helper(("tag_type",))

class UnknownData (Part):
    def __repr__(self):
        return self._repr_helper(())

def _parse_id3v2_header_footer(header, error_messages):
    major, minor, flags, length = struct.unpack(">3BL", header[3:10])
    if major >= 0xff:
        error_messages.append("invalid major version 0x%x" % (major,))
    if minor >= 0xff:
        error_messages.append("invalid minor version 0x%x" % (minor,))
    if (flags & 0x0f) != 0:
        error_messages.append("invalid flags 0x%x" % (flags,))
    # Add 10 for the header, not counted in length.
    length = (((length & 0x7f000000) >> 3)
              | ((length & 0x007f0000) >> 2)
              | ((length & 0x00007f00) >> 1)
              | (length & 0x0000007f)) + 10
    if flags & 0x10:
        # Tag has footer too.
        length += 10
    logger.debug("ID3v2 header says tag is %d byte(s)", length)
    return flags, length

def _parse_apev2_header_footer(header, error_messages):
    length, num_items, flags, reserved = struct.unpack("<LLLQ", header[12:32])
    if (flags & 0x1ffffff8) != 0:
        error_messages.append("invalid flags 0x%8.8x" % (flags,))
    if reserved != 0:
        error_messages.append("reserved section 0x%16.16x non-zero"
                              % (reserved,))
    if flags & 0x80000000:
        # Tag contains a header, and headers aren't included in the
        # length in the tag.  (Footers are.)
        length += 32
    return flags, length

def _detect_tags(mp3_stream, tags):
    found_tags = False
    while True:
        start_pos = mp3_stream.get_file_position(False)
        error_messages = []
        # ID3v1 header: 3 bytes
        # ID3v2 header: 10 bytes
        # APEv2 header: 32 bytes
        #
        # APEv2 has the biggest header, so read in 32 bytes.
        #
        # Note Lyrics3v2 doesn't have a header and is supposed to come
        # in the footer, so we don't bother with it here.
        header = mp3_stream.peek(32)
        if header.startswith("TAG"):
            tag_type, header_length, length = Tag.ID3V1, 3, 128
        elif header.startswith("ID3") and len(header) >= 10:
            tag_type, header_length = Tag.ID3V2, 10
            flags, length = _parse_id3v2_header_footer(header, error_messages)
        # We match the APEv2 version bytes as well.  You might have an
        # APEv1 tag but we don't handle those and so I want to avoid
        # reporting it as an invalid APEv2 tag.
        elif (header.startswith("APETAGEX\xd0\x07\x00\x00")
              and len(header) >= 32):
            tag_type, header_length = Tag.APEV2, 32
            flags, length = _parse_apev2_header_footer(header, error_messages)
            if not (flags & 0x20000000):
                error_messages.append("APEv2 footer found without header, not"
                                      " at end of file, skipping stray footer")
                # Just skip this stray footer.
                length = 32
            elif not (flags & 0x80000000):
                error_messages.append("APEv2 header says tag doesn't have a"
                                      " header")
        else:
            # Not looking at a tag of any known type.
            break
        if not mp3_stream.consume_exactly(length):
            # Bad length in header?  Just skip the header.
            error_messages.append(("header says tag length is %d byte(s) but"
                                   " there aren't that many bytes available,"
                                   " skipping just header") % (length,))
            mp3_stream.consume_exactly(header_length)
        logger.debug("gathered %s tag", tag_type)
        tags.append(Tag(start=start_pos,
                        end=mp3_stream.get_file_position(False),
                        tag_type=tag_type, error_messages=error_messages))
        found_tags = True
        # We could break out here if we didn't read a valid tag (e.g.
        # we threw away just a header), but why not just loop back
        # around and see if we can identify another tag right after
        # that broken header?
    return found_tags

def _detect_tags_at_end(mp3_file_obj):
    mp3_file_obj.seek(0, 2)
    file_pos = mp3_file_obj.tell()
    tags = []
    while True:
        error_messages = []
        # ID3v2 has the smallest footer at 10 bytes.  As far as we're
        # concerned, if you don't have that much to read then you
        # don't have a tag.
        if file_pos < 10:
            break
        # Need to read enough for a whole ID3v1 tag since ID3v1 has no
        # footer, unlike the rest of the tags.
        read_len = min(128, file_pos)
        mp3_file_obj.seek(file_pos - read_len)
        footer = mp3_file_obj.read(read_len)
        assert len(footer) == read_len, repr((footer, len(footer), read_len))
        if footer.startswith("TAG"):
            tag_type, footer_length, length = Tag.ID3V1, 8, 128
        elif footer[-10:-7] == "3DI":
            tag_type, footer_length = Tag.ID3V2, 10
            flags, length = _parse_id3v2_header_footer(footer[-10:],
                                                       error_messages)
            if not (flags & 0x10):
                error_messages.append("ID3v2 footer says tag doesn't have a"
                                      " footer")
        elif footer[-32:-20] == "APETAGEX\xd0\x07\x00\x00":
            tag_type, footer_length = Tag.APEV2, 32
            flags, length = _parse_apev2_header_footer(footer[-32:],
                                                       error_messages)
            if flags & 0x40000000:
                error_messages.append("APEv2 footer says tag doesn't have a"
                                      " footer")
            elif flags & 0x20000000:
                error_messages.append("APEv2 header found without footer near"
                                      " end of file, skipping stray header")
                length = 32
        elif footer.endswith("LYRICS200") and read_len >= 15:
            tag_type, footer_length = Tag.LYRICS3V2, 15
            length_str = footer[-15:-9]
            try:
                length = int(length_str)
            except ValueError:
                error_messages.append(("invalid Lyrics3v2 length %r, skipping"
                                       " footer") % (length_str,))
                length = 15
            else:
                # Length in footer does not include the footer itself.
                length += 15
        else:
            # No tag, we're done.
            break
        if length > file_pos:
            error_messages.append(("invalid tag length %r (only %r preceding"
                                   " bytes), skipping footer")
                                  % (length, file_pos))
            assert footer_length <= read_len, repr((footer_length, read_len))
            length = footer_length
        tag_end = file_pos
        file_pos -= length
        tags.append(Tag(start=file_pos, end=tag_end, tag_type=tag_type,
                        error_messages=error_messages))
    tags.reverse()
    return tags

class _TruncatingFileWrapper (object):
    def __init__(self, file_obj, truncated_len):
        self._file_obj = file_obj
        self._truncated_len = truncated_len
        bytes_remaining = truncated_len - file_obj.tell()
        assert bytes_remaining >= 0, repr(file_obj.tell(), truncated_len)
        self._bytes_remaining = bytes_remaining
        self.tell = file_obj.tell

    def readinto(self, view):
        bytes_remaining = self._bytes_remaining
        if len(view) > bytes_remaining:
            if not isinstance(view, memoryview):
                view = memoryview(view)
            view = view[:self._bytes_remaining]
        bytes_read = self._file_obj.readinto(view)
        bytes_remaining -= bytes_read
        assert bytes_remaining >= 0, repr(bytes_remaining, bytes_read)
        self._bytes_remaining = bytes_remaining
        return bytes_read

    def seek(self, offset, whence=0):
        truncated_len = self._truncated_len
        bytes_remaining = self._bytes_remaining
        if ((whence == 0 and offset > truncated_len)
            or (whence == 1 and offset > bytes_remaining)
            or (whence == 2 and offset > 0)):
            raise IOError("can't seek past EOF of truncated file")
        if whence == 2:
            whence = 0
            offset = truncated_len + offset
        self._file_obj.seek(offset, whence)
        if whence == 0:
            bytes_remaining = truncated_len - offset
        else:
            bytes_remaining += -offset
        assert bytes_remaining >= 0, bytes_remaining
        self._bytes_remaining = bytes_remaining

def scan_mp3(mp3_file_obj):
    footer_parts = _detect_tags_at_end(mp3_file_obj)
    if footer_parts:
        start_of_footer_tags = footer_parts[0].start
    else:
        mp3_file_obj.seek(0, 2)
        start_of_footer_tags = mp3_file_obj.tell()
    mp3_file_obj.seek(0)
    limited_file_obj = _TruncatingFileWrapper(mp3_file_obj,
                                              start_of_footer_tags)
    mad_stream = madctypes.mad_stream()
    mad_frame = madctypes.mad_frame()
    mp3_stream = _MP3Stream(limited_file_obj, mad_stream)
    mp3_stream.feed_mad()
    lost_sync_start = None
    last_position = mp3_stream.get_file_position(True)
    mp3_parts = []
    while True:
        try:
            madctypes.mad_frame_decode(mad_frame, mad_stream)
        except madctypes.MADStreamError, ex:
            error_code = ex.error_code
            if error_code == madctypes.MAD_ERROR_BUFLEN:
                if mp3_stream.feed_mad():
                    # Need to restart with refilled buffer.
                    continue
                else:
                    # EOF
                    break
            elif error_code == madctypes.MAD_ERROR_LOSTSYNC:
                # Have to check lost_sync_start since rebuffering
                # resets mad_stream.sync=1.
                logger.debug("MAD_ERROR_LOSTSYNC, searching for tags")
                if (lost_sync_start is None
                    and not _detect_tags(mp3_stream, mp3_parts)):
                    lost_sync_start = last_position
                    logger.debug(("MAD_ERROR_LOSTSYNC, no tags found,"
                                  " set lost_sync_start=%d"),
                                 lost_sync_start)
                else:
                    # We've advanced the stream, last_position needs
                    # to point at the place MAD will next read
                    # (i.e. the start of the next frame, or of some
                    # unknown data).
                    last_position = mp3_stream.get_file_position(True)
                    logger.debug(("MAD_ERROR_LOST_SYNC, found tags,"
                                  " set last_position=%d"), last_position)
                # Can't continue on with this loop since it ends with
                # "write an audio frame" but we don't have an audio
                # frame.
                continue
            elif not madctypes.MAD_RECOVERABLE(error_code):
                raise
        else:
            header = mad_frame.header
            ex = None
        if lost_sync_start is not None:
            # Set last_position to be position at which we resynched.
            # last_position will thus be used as the end of the
            # unknown data, and in just a few more lines used again as
            # the start of the audio frame.
            last_position = mp3_stream.get_frame_start_position()
            logger.debug("regained sync at %d", last_position)
            mp3_parts.append(UnknownData(lost_sync_start, last_position))
            lost_sync_start = None
        if ex:
            error_messages = [ex.error_message]
        else:
            error_messages = ()
        new_position = mp3_stream.get_file_position(True)
        header = mad_frame.header
        mp3_parts.append(AudioFrame(
            start=last_position,
            end=new_position,
            error_messages=error_messages,
            layer=header.layer,
            mode=header.mode,
            bit_rate=header.bitrate,
            sampling_rate=header.samplerate,
            ))
        last_position = new_position
    last_position = (lost_sync_start if lost_sync_start is not None
                     else last_position)
    if last_position < start_of_footer_tags:
        mp3_parts.append(UnknownData(start=last_position,
                                     end=start_of_footer_tags))
    mp3_parts.extend(footer_parts)
    assert mp3_parts[0].start == 0, repr(mp3_parts[0].start)
    assert all(mp3_parts[i - 1].end == mp3_parts[i].start
               for i in xrange(1, len(mp3_parts)))
    return mp3_parts

class _MP3Stream (object):
    # These numbers chosen with good intentions and no clue.  Note
    # that the buffer size will most commonly be doubled each time it
    # needs to grow bigger, so keeping INITIAL_BUFFER_SIZE and
    # MAX_BUFFER_SIZE as powers of two is recommended.
    INITIAL_BUFFER_SIZE = 2**16
    MAX_BUFFER_SIZE = 2**20
    MIN_READ_SIZE = INITIAL_BUFFER_SIZE / 2

    def __init__(self, file_obj, mad_stream):
        self._file_obj = file_obj
        self._mad_stream = mad_stream
        self._buf_size = buf_size = self.INITIAL_BUFFER_SIZE
        # _orig_buf must be kept around so that its underlying buffer
        # doesn't get garbage collected.  However, when we use
        # ctypes.resize, _buf will still reflect the old, smaller
        # size.  Therefore _buf will always point at a c_char array of
        # the appropriate size.  _orig_buf is, therefore, only used
        # when resizing.
        buf = ctypes.create_string_buffer(buf_size)
        self._orig_buf = buf
        self._buf = buf
        self._view = memoryview(buf)
        self._buf_file_position = file_obj.tell()
        self._buffer_guard_length = 0

    def get_file_position(self, for_mad):
        """Returns the next buffer position as an input file offset.

        The "next buffer position" is determined partially based on
        for_mad: if true then you are requesting the offset of the
        next byte that MAD will consider, if false then you are
        requesting the offset of the next byte that peek and
        consume_exactly will operate upon.

        Since this method returns an offset into the input file, it
        will never reflect buffer guard bytes.  If the current
        position is beyond the end of the file (i.e. in the buffer
        guard) then this method will return the last offset within the
        input file.

        """
        mad_stream = self._mad_stream
        skiplen = mad_stream.skiplen
        if for_mad and (mad_stream.sync or not skiplen):
            buf_position = mad_stream.next_frame_offset
        else:
            buf_position = mad_stream.this_frame_offset
        buf_position += skiplen
        buffer_length = mad_stream.buffer_length
        assert buf_position <= buffer_length, (buf_position, buffer_length)
        buffer_length -= self._buffer_guard_length
        return self._buf_file_position + min(buf_position, buffer_length)

    def get_frame_start_position(self):
        """Returns file offset of the stream's current frame.

        You probably don't want to call this unless the stream is
        synchronized.

        """
        mad_stream = self._mad_stream
        assert mad_stream.sync
        return self._buf_file_position + mad_stream.this_frame_offset

    def _ensure_free(self, bytes_in_buf, min_bytes_free):
        buf_size = self._buf_size
        bytes_free = buf_size - bytes_in_buf
        if bytes_free < min_bytes_free:
            bytes_needed = min_bytes_free - bytes_free
            new_buf_size = max(buf_size * 2, buf_size + bytes_needed)
            if new_buf_size > self.MAX_BUFFER_SIZE:
                raise Exception("%r exceeds max buffer size %r"
                                % (new_buf_size, self.MAX_BUFFER_SIZE))
            orig_buf = self._orig_buf
            ctypes.resize(orig_buf, new_buf_size)
            new_buf_type = ctypes.c_char * new_buf_size
            buf_address = ctypes.addressof(orig_buf)
            self._buf = buf = new_buf_type.from_address(buf_address)
            self._view = view = memoryview(buf)
            self._buf_size = new_buf_size
        else:
            view = self._view
        # Return view as a convenience.
        return view

    def _read_into_buf(self, num_bytes_in_buf, num_additional_bytes_wanted):
        free_bytes_needed = max(num_additional_bytes_wanted,
                                self.MIN_READ_SIZE)
        view = self._ensure_free(num_bytes_in_buf, free_bytes_needed)
        append_view = view[num_bytes_in_buf:]
        bytes_added = self._file_obj.readinto(append_view)
        assert bytes_added >= 0, repr(bytes_added)
        return bytes_added

    def _add_buffer_guard(self, num_bytes_in_buf):
        buffer_guard_length = madctypes.MAD_BUFFER_GUARD
        slice_start = num_bytes_in_buf
        slice_end = slice_start + buffer_guard_length
        buffer_guard = "\0" * buffer_guard_length
        self._view[slice_start:slice_end] = buffer_guard
        self._buffer_guard_length = buffer_guard_length
        # Returned as a convenience for _ensure_available.
        return buffer_guard_length

    def _ensure_available(self, for_mad, num_bytes=None):
        """Tries to read at least num_bytes in to the buffer.

        If num_bytes is None (default) then at least
        self.MIN_READ_SIZE additional bytes will be read.

        Returns number of bytes added to the buffer and the total
        number of bytes available in the buffer.  These numbers may
        include buffer guard bytes.

        If for_mad is true then everything in the current buffer up to
        next_frame + skiplen has been consumed.  If for_mad is false
        then everything in the current buffer up to this_frame +
        skiplen has been consumed.

        """
        mad_stream = self._mad_stream
        if for_mad:
            consumed_bytes = mad_stream.next_frame_offset
        else:
            consumed_bytes = mad_stream.this_frame_offset
        consumed_bytes += mad_stream.skiplen
        # Note "available" here means "unconsumed data" (not "free
        # space").
        available_bytes = mad_stream.buffer_length - consumed_bytes
        assert available_bytes >= 0, repr((mad_stream.buffer_length,
                                           consumed_bytes, mad_stream.skiplen))
        if num_bytes is None:
            bytes_needed = self.MIN_READ_SIZE
        else:
            bytes_needed = num_bytes - available_bytes
        if bytes_needed > 0 and not self._buffer_guard_length:
            if available_bytes:
                available_ptr = mad_stream.bufend - available_bytes
                ctypes.memmove(self._buf, available_ptr, available_bytes)
            self._buf_file_position += consumed_bytes
            bytes_added = self._read_into_buf(available_bytes, bytes_needed)
            if not bytes_added:
                bytes_added = self._add_buffer_guard(available_bytes)
            available_bytes += bytes_added
            # Note self._buf might be a different object from the
            # reference earlier in this function due to
            # _read_into_buf.
            madctypes.mad_stream_buffer(mad_stream, self._buf, available_bytes)
            # We already took skiplen into account above.
            mad_stream.skiplen = 0
        else:
            bytes_added = 0
        return bytes_added, available_bytes

    def peek(self, num_bytes):
        """Returns substring of buffer up to num_bytes in length.

        This can only be called when MAD has lost synchronization and
        this method will return bytes starting with the byte where MAD
        lost synchronization.  This method will not return buffer
        guard bytes.

        """
        mad_stream = self._mad_stream
        bytes_added, bytes_in_buf = self._ensure_available(False,
                                                           num_bytes=num_bytes)
        bytes_in_buf -= self._buffer_guard_length
        num_bytes_returned = min(num_bytes, bytes_in_buf)
        buf_position = mad_stream.this_frame_offset + mad_stream.skiplen
        return self._buf[buf_position:buf_position + num_bytes_returned]

    def consume_exactly(self, num_bytes):
        """Returns true if num_bytes can be discarded from the input stream.

        If num_bytes are not available, returns false.

        This can only be called when MAD has lost synchronization and
        this method will consume bytes starting with the byte where
        MAD lost synchronization.  This method will not consume buffer
        guard bytes.

        """
        mad_stream = self._mad_stream
        buf_position = mad_stream.this_frame_offset + mad_stream.skiplen
        buf_length = mad_stream.buffer_length
        bytes_in_buf = buf_length - buf_position - self._buffer_guard_length
        assert bytes_in_buf >= 0, \
            repr((buf_length, buf_position, self._buffer_guard_length))
        if num_bytes > bytes_in_buf:
            advance_file_bytes = num_bytes - bytes_in_buf
            # This depends on _file_obj being a
            # _TruncatingFileWrapper, which raises an error if you try
            # to seek beyond EOF.
            try:
                self._file_obj.seek(advance_file_bytes, 1)
            except EnvironmentError:
                return False
            else:
                assert not self._buffer_guard_length, \
                    "added buffer guard but not at EOF?"
                # Skip the rest of the buffer and increment
                # _buf_file_position by the amount we advanced the
                # file.  file_position should still be accurate: this
                # method's starting point plus skiplen plus
                # advance_file_bytes (by way of the use of
                # _buf_file_position in _file_position).  (A problem
                # would arise if we ever wanted to look backwards, but
                # thankfully we don't.)
                madctypes.mad_stream_skip(mad_stream, bytes_in_buf)
                self._buf_file_position += advance_file_bytes
        else:
            madctypes.mad_stream_skip(mad_stream, num_bytes)
        return True

    def feed_mad(self):
        """Returns number of new bytes inserted into MAD's buffer

        Returned number of bytes includes any buffer guard bytes.

        """
        bytes_added, bytes_in_buf = self._ensure_available(True)
        return bytes_added

if __name__ == "__main__":
    import sys
    import pprint
    _logging.basicConfig()
    pprint.pprint(scan_mp3(open(sys.argv[1], "rb")))
