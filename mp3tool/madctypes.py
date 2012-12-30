from ctypes import *
from ctypes.util import find_library

__all__ = ["mad_stream", "mad_frame", "MADStreamError",
           "mad_stream_buffer", "mad_stream_skip", "mad_frame_decode"]

c_ubyte_p = POINTER(c_ubyte)
# MAD defines lots of stuff as unsigned char *, but it's more
# convenient (and hopefully harmless) for us to tell ctypes that these
# are c_char_p.  It will then take a char array (i.e. result of
# create_string_buffer) without complaint/casting.  We only use this
# for pointers into the input buffer, the only pointers we care about.
mad_buffer_p = c_void_p

libmad = CDLL(find_library("mad"))

MAD_BUFFER_GUARD = 8
MAD_BUFFER_MDLEN = (511 + 2048 + MAD_BUFFER_GUARD)

if sizeof(c_int) >= 4:
    mad_fixed_t = c_int
else:
    mad_fixed_t = c_long

# Types we're lying about.
enum_type = c_uint
mad_layer_enum = enum_type
mad_mode_enum = enum_type
mad_emphasis_enum = enum_type

# mad_error_enum
mad_error_enum = enum_type
MAD_ERROR_NONE           = 0x0000

MAD_ERROR_BUFLEN         = 0x0001
MAD_ERROR_BUFPTR         = 0x0002

MAD_ERROR_NOMEM          = 0x0031

MAD_ERROR_LOSTSYNC       = 0x0101
MAD_ERROR_BADLAYER       = 0x0102
MAD_ERROR_BADBITRATE     = 0x0103
MAD_ERROR_BADSAMPLERATE  = 0x0104
MAD_ERROR_BADEMPHASIS    = 0x0105

MAD_ERROR_BADCRC         = 0x0201
MAD_ERROR_BADBITALLOC    = 0x0211
MAD_ERROR_BADSCALEFACTOR = 0x0221
MAD_ERROR_BADMODE        = 0x0222
MAD_ERROR_BADFRAMELEN    = 0x0231
MAD_ERROR_BADBIGVALUES   = 0x0232
MAD_ERROR_BADBLOCKTYPE   = 0x0233
MAD_ERROR_BADSCFSI       = 0x0234
MAD_ERROR_BADDATAPTR     = 0x0235
MAD_ERROR_BADPART3LEN    = 0x0236
MAD_ERROR_BADHUFFTABLE   = 0x0237
MAD_ERROR_BADHUFFDATA    = 0x0238
MAD_ERROR_BADSTEREO      = 0x0239

# Very partial mad_units enum
mad_units_enum = enum_type
MAD_UNITS_MINUTES      = -1
MAD_UNITS_MILLISECONDS = 1000

# mad_mode enum
MAD_MODE_SINGLE_CHANNEL = 0
MAD_MODE_DUAL_CHANNEL   = 1
MAD_MODE_JOINT_STEREO   = 2
MAD_MODE_STEREO         = 3

def MAD_RECOVERABLE(error):
    return error & 0xff00

class mad_bitptr (Structure):
    _fields_ = [("byte", c_ubyte_p),
                ("cache", c_ushort),
                ("left", c_ushort)]

class mad_stream (Structure):
    _fields_ = [("buffer", mad_buffer_p),
                ("bufend", mad_buffer_p),
                ("skiplen", c_ulong),
                ("sync", c_int),
                ("freerate", c_ulong),
                ("this_frame", mad_buffer_p),
                ("next_frame", mad_buffer_p),
                ("ptr", mad_bitptr),
                ("anc_ptr", mad_bitptr),
                ("anc_bitlen", c_uint),
                ("main_data", POINTER(c_ubyte * MAD_BUFFER_MDLEN)),
                ("md_len", c_uint),
                ("options", c_int),
                ("error", c_uint)]

    def __init__(self):
        Structure.__init__(self)
        libmad.mad_stream_init(self)

    def __del__(self):
        libmad.mad_stream_finish(self)

    @property
    def this_frame_offset(self):
        return (self.this_frame or 0) - (self.buffer or 0)

    @property
    def next_frame_offset(self):
        buffer_addr = self.buffer or 0
        next_frame_addr = self.next_frame or 0
        # madlld talks about a NULL next_frame, but I don't think that can
        # actually happen unless (1) you have an error in your code, or
        # (2) right after calling mad_stream_init.
        assert next_frame_addr or next_frame_addr == buffer_addr, \
            repr((buffer_addr, next_frame_addr))
        return next_frame_addr - buffer_addr

    @property
    def buffer_length(self):
        return (self.bufend or 0) - (self.buffer or 0)

class mad_timer_t (Structure):
    _fields_ = [("seconds", c_long),
                ("fraction", c_ulong)]

class mad_header (Structure):
    _fields_ = [("layer", mad_layer_enum),
                ("mode", mad_mode_enum),
                ("mode_extension", c_int),
                ("emphasis", mad_emphasis_enum),
                ("bitrate", c_ulong),
                ("samplerate", c_int),
                ("crc_check", c_ushort),
                ("crc_target", c_ushort),
                ("flags", c_int),
                ("private_bits", c_int),
                ("duration", mad_timer_t)]

class mad_frame (Structure):
    _fields_ = [("header", mad_header),
                ("options", c_int),
                ("sbsample", mad_fixed_t * 2 * 36 * 32),
                ("overlap", POINTER(mad_fixed_t) * 2 * 32 * 18)]

    def __init__(self):
        Structure.__init__(self)
        libmad.mad_frame_init(self)

    def __del__(self):
        libmad.mad_frame_finish(self)

class MADStreamError (Exception):
    def __init__(self, stream):
        assert isinstance(stream, mad_stream), repr(stream)
        self.error_code = error_code = stream.error
        if error_code == 0:
            raise Exception("no error set on mad_stream")
        self.error_message = error_message = libmad.mad_stream_errorstr(stream)
        Exception.__init__(self, "libmad error 0x%x: %s" % (error_code,
                                                            error_message))

    @classmethod
    def errcheck(cls, result, function, arguments):
        if function == libmad.mad_frame_decode:
            if result != 0:
                raise cls(arguments[1])
        else:
            raise Exception("MADStreamError.errcheck for unknown function %r"
                            % (function,))
        return result

libmad.mad_stream_init.argtypes = [POINTER(mad_stream)]
libmad.mad_stream_init.restype = None

libmad.mad_stream_finish.argtypes = [POINTER(mad_stream)]
libmad.mad_stream_finish.restype = None

libmad.mad_frame_init.argtypes = [POINTER(mad_frame)]
libmad.mad_frame_init.restype = None

libmad.mad_frame_finish.argtypes = [POINTER(mad_frame)]
libmad.mad_frame_finish.restype = None

libmad.mad_stream_errorstr.argtypes = [POINTER(mad_stream)]
libmad.mad_stream_errorstr.restype = c_char_p

mad_stream_buffer = libmad.mad_stream_buffer
mad_stream_buffer.argtypes = [POINTER(mad_stream), mad_buffer_p, c_ulong]
mad_stream_buffer.restype = None

mad_stream_skip = libmad.mad_stream_skip
mad_stream_skip.argtypes = [POINTER(mad_stream), c_ulong]
mad_stream_skip.restype = None

mad_frame_decode = libmad.mad_frame_decode
mad_frame_decode.argtypes = [POINTER(mad_frame), POINTER(mad_stream)]
mad_frame_decode.restype = c_int
mad_frame_decode.errcheck = MADStreamError.errcheck
