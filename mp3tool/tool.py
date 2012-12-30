#!/usr/bin/env python2.7
#
# TODO:
# - Tag operations like tag-info are too slow because they decode the
#   whole file.  Might be better to implement selectively poking a
#   file to see if the tag is where we expect it to be.
# - Syntax for "edit" command doesn't lend itself to applying the same
#   transforms to many files, which is bad.
# - All those nargs="+" options should be switched to comma-separated
#   parsing.  Sorry, argparse.
# - Should be able to specify exactly which problems you want to fix,
#   rather than having to fix all of them blindly.
# - "edit" should support e.g. "apev2" when you don't care about valid
#   vs. invalid.
# - Need to check for checksums in MP3 headers.
# - Could probably use some mode(s) to tell whether a whole set of
#   files have similar/same parameters.
# - Warn on empty tags.

import sys
import argparse
import logging as _logging
import functools
import tempfile
import os.path
import os
import operator

from clint.textui import colored
import mutagen.id3
import mutagen.apev2

from mp3tool.scanner import (scan_mp3, AudioFrame, Tag, UnknownData,
                             MODE_SINGLE_CHANNEL, MODE_DUAL_CHANNEL,
                             MODE_JOINT_STEREO, MODE_STEREO)

logger = _logging.getLogger("mp3tool.tool")

def abstractmethod(method):
    @functools.wraps(method)
    def abstract(*args, **kwargs):
        raise NotImplementedError("you must implement me")
    return abstract

def abstractstaticmethod(method):
    return staticmethod(abstractmethod(method))

class Task (object):
    @staticmethod
    def add_arguments(parser):
        pass

    @abstractstaticmethod
    def run(options, mp3_file, reporter):
        pass

class PartIterator (object):
    _DELETED = object()

    def __init__(self, parts, start=None, end=None, part_type=None):
        assert start is None or start >= 0
        assert end is None or end <= len(parts)
        assert start <= end
        self._parts = parts
        self._last_part = None
        self._next_index = start or 0
        self._end_index = end or len(parts)
        self._part_type = part_type

    def __iter__(self):
        return self

    def next(self):
        parts, part_type = self._parts, self._part_type
        # Loop may execute zero times, in which case next_index would
        # never be assigned by the loop.  We may use next_index after
        # the loop, hence the reason for this assignment.
        next_index = self._next_index
        for next_index in xrange(next_index, self._end_index):
            part = parts[next_index]
            if not part_type or isinstance(part, part_type):
                break
        else:
            raise StopIteration
        self._last_part = part
        self._next_index = next_index + 1
        return part

    def delete_last(self):
        last_part = self._last_part
        if last_part is None:
            raise Exception("you've never requested a part")
        elif self._last_part is not self._DELETED:
            parts = self._parts
            next_index = self._next_index
            last_index = next_index - 1
            if self._last_part is not parts[last_index]:
                raise Exception("externally modified")
            del parts[last_index]
            self._last_part = self._DELETED
            self._next_index = next_index - 1
            self._end_index -= 1

class PartGroupIterator (object):
    def __init__(self, parts, key_func, part_type=None):
        self._parts = parts
        self._key_func = key_func
        self._part_type = part_type
        self._last_start = None
        self._next_start = 0

    def __iter__(self):
        return self

    def next(self):
        start = self._next_start
        parts = self._parts
        key_func = self._key_func
        part_type = self._part_type
        len_parts = len(parts)
        for start in xrange(start, len_parts):
            part = parts[start]
            if not part_type or isinstance(part, part_type):
                break
        else:
            raise StopIteration
        first_key = key_func(part)
        end = start + 1
        while end < len_parts:
            part = parts[end]
            if ((part_type and not isinstance(part, part_type))
                or key_func(parts[end]) != first_key):
                break
            else:
                end += 1
        self._last_start, self._next_start = start, end
        return first_key, start, end

    def delete_last(self):
        start, end = self._last_start, self._next_start
        assert end > start, repr((start, end))
        if start is None:
            raise Exception("never started or already deleted")
        del self._parts[start:end]
        self._last_start, self._next_start = None, start

# This is a (very) partial implementation of the interface for tags
# from Mutagen, since Mutagen only directly supports ID3v1 if ID3v2
# isn't present.  It uses functions I'm pretty sure aren't meant to be
# part of the Mutagen public interface.  It absolutely won't work on a
# file that doesn't already have an ID3v1 tag.
class ID3v1Wrapper (dict):
    def __init__(self, mp3_file_path):
        self._mp3_file_path = mp3_file_path
        mp3_file_obj = open(mp3_file_path, "rb")
        mp3_file_obj.seek(-128, 2)
        id3v1_bytes = mp3_file_obj.read(128)
        assert len(id3v1_bytes) == 128, len(id3v1_bytes)
        assert id3v1_bytes.startswith("TAG"), repr(id3v1_bytes)
        mp3_file_obj.close()
        dict.__init__(self, mutagen.id3.ParseID3v1(id3v1_bytes))

    # Argument named filename instead of mp3_file_path to be more
    # compatible with Mutagen's interface.
    def save(self, filename=None):
        if filename is None:
            filename = self._mp3_file_path
        # Note that we require filename to exist, which seems reasonable.
        mp3_file_obj = open(filename, "r+b")
        try:
            mp3_file_obj.seek(-128, 2)
        except IOError:
            logger.debug("not even 128 bytes in file")
            mp3_file_obj.seek(0, 2)
        else:
            logger.debug("looking for ID3v1 at %r", mp3_file_obj.tell())
            if mp3_file_obj.read(3) == "TAG":
                mp3_file_obj.seek(-3, 1)
            else:
                mp3_file_obj.seek(0, 2)
        logger.debug("writing ID3v1 tag at %r", mp3_file_obj.tell())
        id3v1_bytes = mutagen.id3.MakeID3v1(self)
        mp3_file_obj.write(id3v1_bytes)
        mp3_file_obj.close()

class MP3File (object):
    _CHUNK_SIZE = 65536

    def __init__(self, mp3_file_path):
        self.mp3_file_path = mp3_file_path
        mp3_file_obj = open(mp3_file_path, "rb")
        self.parts = scan_mp3(mp3_file_obj)
        mp3_file_obj.close()
        self._ignored_tags = set()
        self._tags = {}

    def iter_parts(self, *args, **kwargs):
        return PartIterator(self.parts, *args, **kwargs)

    def iter_part_groups(self, *args, **kwargs):
        return PartGroupIterator(self.parts, *args, **kwargs)

    def _copy_file_data(self, source_file_obj, dest_file_obj, num_bytes):
        while num_bytes > 0:
            data = source_file_obj.read(min(num_bytes, self._CHUNK_SIZE))
            dest_file_obj.write(data)
            num_bytes -= len(data)

    def update_file(self, output_path=None):
        """

        Specifying output_path updates self.mp3_file_path to be equal
        to output_path, since after this method returns its data may
        no longer reflect the input file.


        """
        input_path = self.mp3_file_path
        input_file_obj = open(input_path, "rb")
        mp3_dir_path, mp3_file_name = os.path.split(input_path)
        try:
            if (output_path and os.path.exists(output_path)
                and os.path.samefile(input_path, output_path)):
                output_path = None
            if output_path is None:
                fd, temp_file_path = tempfile.mkstemp(
                    prefix="." + mp3_file_name,
                    suffix=".tmp",
                    dir=mp3_dir_path,
                    )
                output_file_obj = os.fdopen(fd, "wb")
            else:
                output_file_obj = open(output_path, "w")
            start_pos = 0
            last_end_pos = 0
            adjustment = 0
            for part in self.parts:
                assert part.start >= last_end_pos, \
                    repr((part.start, last_end_pos))
                if part.start != last_end_pos:
                    self._copy_file_data(input_file_obj, output_file_obj,
                                         last_end_pos - start_pos)
                    start_pos = part.start
                    input_file_obj.seek(start_pos)
                    adjustment += start_pos - last_end_pos
                part.start -= adjustment
                last_end_pos = part.end
                part.end -= adjustment
            self._copy_file_data(input_file_obj, output_file_obj,
                                 last_end_pos - start_pos)
            input_file_obj.close()
            output_file_obj.close()
            if output_path is None:
                # XXX This fails on Windows because mp3_file_path
                # exists, per Python 2.7 documentation.  Want to be
                # nice to Windows users?
                os.rename(temp_file_path, input_path)
            else:
                self.mp3_file_path = output_path
        except:
            if "temp_file_path" in locals() and os.path.exists(temp_file_path):
                os.unlink(temp_file_path)
            raise
        # Saving tags after our work since our work above is done
        # based on byte offsets, and Mutagen resizing a tag during
        # that would totally screw us up.
        tags = self._tags
        for tag in tags.itervalues():
            # XXX Bug here: if we were to try and read the parts after
            # updating tags, the offsets could still be off because
            # Mutagen changed the size of one or more tags.
            tag.save(output_path or input_path)
        # Let's just re-read these from the new file.
        tags.clear()

    @staticmethod
    def _is_tag_type(part, tag_type):
        return isinstance(part, Tag) and part.tag_type == tag_type

    def get_tag(self, tag_type):
        if tag_type in self._ignored_tags:
            return None
        tags = self._tags
        tag = tags.get(tag_type)
        if tag:
            return tag
        mp3_file_path = self.mp3_file_path
        parts = self.parts
        if tag_type == Tag.ID3V1:
            last_part = parts[-1]
            if self._is_tag_type(last_part, Tag.ID3V1):
                tag = ID3v1Wrapper(mp3_file_path)
        elif tag_type == Tag.ID3V2:
            first_part = parts[0]
            if self._is_tag_type(first_part, Tag.ID3V2):
                tag = mutagen.id3.ID3(mp3_file_path)
                if tag and tag.version < (2, 0, 0):
                    tag = None
        elif tag_type == Tag.APEV2:
            index = -1
            tags_to_eat = [Tag.LYRICS3V2, Tag.ID3V1]
            while tags_to_eat:
                part = parts[index]
                if not isinstance(part, Tag):
                    break
                elif part.tag_type != tags_to_eat.pop():
                    break
                else:
                    index -= 1
            part = parts[index]
            if self._is_tag_type(part, Tag.APEV2):
                tag = mutagen.apev2.APEv2(mp3_file_path)
        else:
            raise Exception("unknown/unsupported tag type %r" % (tag_type,))
        if tag:
            tags[tag_type] = tag
        return tag

    # XXX is this needed now that get_tag checks tag positions?
    def ignore_tag(self, tag_type):
        self._ignored_tags.add(tag_type)
        # This tag type is dead to us, so we won't need the cached
        # version of the tag ever again--and we need to remove it from
        # this collection so that it doesn't get .save'd in
        # update_file (which could make a deleted tag reappear after
        # update_file, which would be confusing and bad).
        self._tags.pop(tag_type, None)

    def get_average_bit_rate(self):
        bit_rate_sum, num_frames = 0, 0
        for part in self.iter_parts(part_type=AudioFrame):
            bit_rate_sum += part.bit_rate
            num_frames += 1
        if num_frames:
            return bit_rate_sum / float(num_frames)
        else:
            return 0

class Reporter (object):
    _WARNING = colored.yellow("warning")
    _ERROR = colored.red("error")
    _REPAIRED = colored.green("repaired")

    def __init__(self, mp3_file):
        self._mp3_file = mp3_file

    def _report(self, report_type, location, message, args):
        mp3_file = self._mp3_file
        buf = ["%s:%s:" % (report_type, mp3_file.mp3_file_path)]
        if isinstance(location, tuple):
            start, end = location
            buf.append("%d+%d frame(s):" % (mp3_file.parts[start].start,
                                            end - start))
        elif location:
            buf.append("%d+%d byte(s):" % (location.start,
                                           location.end - location.start))
        # XXX I think this is a bit hackish
        interpolated_message = " %s\n" % (message % args,)
        if isinstance(interpolated_message, unicode):
            interpolated_message = interpolated_message.encode("utf-8")
        buf.append(interpolated_message)
        sys.stderr.write("".join(buf))

    def file_warning(self, message, *args):
        self._report(self._WARNING, None, message, args)

    def file_error(self, message, *args):
        self._report(self._ERROR, None, message, args)

    def file_repaired(self, message, *args):
        self._report(self._REPAIRED, None, message, args)

    def part_warning(self, part, message, *args):
        self._report(self._WARNING, part, message, args)

    def part_error(self, part, message, *args):
        self._report(self._ERROR, part, message, args)

    def part_repaired(self, part, message, *args):
        self._report(self._REPAIRED, part, message, args)

    def range_warning(self, start, end, message, *args):
        self._report(self._WARNING, (start, end), message, args)

    def range_error(self, start, end, message, *args):
        self._report(self._ERROR, (start, end), message, args)

    def range_repaired(self, start, end, message, *args):
        self._report(self._REPAIRED, (start, end), message, args)

class InvalidDataRule (Task):
    @staticmethod
    def run(options, mp3_file, reporter):
        first_message = "invalid audio frames, errors follow:"
        try_repair = options.try_repair
        if try_repair:
            first_message = "deleted " + first_message
            report = reporter.range_repaired
        else:
            report = reporter.range_error
        key_func = operator.attrgetter("error_messages")
        iterator = mp3_file.iter_part_groups(key_func, part_type=AudioFrame)
        for error_messages, start, end in iterator:
            if error_messages:
                if try_repair:
                    iterator.delete_last()
                report(start, end, first_message)
                for error_message in error_messages:
                    report(start, end, error_message)

class DeleteToRepairRule (Task):
    @classmethod
    def run(cls, options, mp3_file, reporter):
        iterator = mp3_file.iter_parts()
        predicate = cls._predicate
        for part in iterator:
            if predicate(part):
                if options.try_repair:
                    iterator.delete_last()
                    deleted = True
                else:
                    deleted = False
                cls._report(reporter, part, deleted)

    @abstractstaticmethod
    def _predicate(part):
        pass

    @abstractstaticmethod
    def _report(reporter, part, was_deleted):
        pass

class UnknownDataRule (DeleteToRepairRule):
    @staticmethod
    def _predicate(part):
        return isinstance(part, UnknownData)

    @staticmethod
    def _report(reporter, part, was_deleted):
        if was_deleted:
            report, prefix = reporter.part_repaired, "deleted "
        else:
            report, prefix = reporter.part_warning, ""
        report(part, "%sunknown data", prefix)

class InvalidTagsRule (DeleteToRepairRule):
    @staticmethod
    def _predicate(part):
        return isinstance(part, Tag) and not part.valid

    @staticmethod
    def _report(reporter, part, was_deleted):
        if was_deleted:
            report_method = reporter.part_repaired
            message_prefix = "deleted "
        else:
            report_method = reporter.part_error
            message_prefix = ""
        report_method(part, "%sinvalid %s tag, errors follow:", message_prefix,
                      part.tag_type)
        for message in part.error_messages:
            report_method(part, message)

class ExpectedTagsRule (Task):
    @staticmethod
    def add_arguments(parser):
        for tag_type, default in (("ID3v1", "optional"),
                                  ("ID3v2", "required"),
                                  ("APEv2", "optional")):
            parser.add_argument(
                "--%s" % (tag_type.lower(),),
                choices=("required", "optional", "forbidden"),
                default=default,
                help=("Are %s tags required, optional, or forbidden?"
                      "  (Default: %%(default)s)") % (tag_type,),
                )
        # I don't want to mess around with trying to support Lyrics3v2
        # right now.
        parser.set_defaults(lyrics3v2="forbidden")

    @staticmethod
    def _look_for_tag(options, mp3_file, part_index, tag_type, reporter):
        option = getattr(options, tag_type.lower())
        parts = mp3_file.parts
        part = parts[part_index]
        tag_found = (isinstance(part, Tag) and part.tag_type == tag_type)
        if option == "required" and not tag_found:
            reporter.file_error("missing %s tag", tag_type)
            # Make sure we don't even try to retrieve this tag, which
            # could lead to us retrieving the tag from a non-standard
            # (or "unapproved") location within the file.
            mp3_file.ignore_tag(tag_type)
        elif option == "forbidden" and tag_found:
            if options.try_repair:
                del parts[part_index]
                reporter.part_repaired(part, "deleted %s tag", tag_type)
            else:
                reporter.part_error(part, "forbidden %s tag", tag_type)
            mp3_file.ignore_tag(tag_type)
        return tag_found

    @classmethod
    def run(cls, options, mp3_file, reporter):
        if cls._look_for_tag(options, mp3_file, 0, Tag.ID3V2, reporter):
            start_index = 1
        else:
            start_index = 0
        end_index = len(mp3_file.parts)
        # The ordering here is important, starting from the last type
        # of tag we expect to see in a file and working backwards
        # through the file.
        for tag_type in (Tag.ID3V1, Tag.LYRICS3V2, Tag.APEV2):
            if cls._look_for_tag(options, mp3_file, end_index - 1, tag_type,
                                 reporter):
                end_index -= 1
        iterator = mp3_file.iter_parts(start_index, end_index, Tag)
        message = "%s tag at unexpected position"
        try_repair = options.try_repair
        if try_repair:
            message = "deleted " + message
            report = reporter.part_repaired
        else:
            report = reporter.part_error
        for part in iterator:
            if try_repair:
                iterator.delete_last()
            report(part, message, part.tag_type)

class BitRateRule (Task):
    @staticmethod
    def add_arguments(parser):
        parser.add_argument("-b", "--min-kbps", type=int, default=161,
                            metavar="KBPS",
                            help=("Minimum average bit rate."
                                  "  Default: %(default)s"))

    @staticmethod
    def run(options, mp3_file, reporter):
        average_kbps = mp3_file.get_average_bit_rate() / 1000
        if average_kbps <= options.min_kbps:
            reporter.file_warning(
                "average bit rate %dkbps less than minimum %dkbps",
                average_kbps,
                options.min_kbps,
                )

MODE_TO_LABEL = {
    MODE_SINGLE_CHANNEL: "single channel",
    MODE_DUAL_CHANNEL: "dual channel",
    MODE_JOINT_STEREO: "joint stereo",
    MODE_STEREO: "stereo",
    }

class ChannelModesRule (Task):
    OPTION_VALUE_TO_MODE = {
        "single": MODE_SINGLE_CHANNEL,
        "dual": MODE_DUAL_CHANNEL,
        "joint": MODE_JOINT_STEREO,
        "stereo": MODE_STEREO,
        }

    @classmethod
    def add_arguments(cls, parser):
        parser.add_argument("--allowed-channel-modes", metavar="MODE",
                            dest="allowed_channel_modes",
                            nargs="+", default=["joint", "stereo"],
                            choices=sorted(cls.OPTION_VALUE_TO_MODE),
                            help=("One or more permitted channel modes."
                                  "  Valid values: %(choices)s."
                                  "  Default: %(default)s"))

    @classmethod
    def run(cls, options, mp3_file, reporter):
        allowed_modes = set(cls.OPTION_VALUE_TO_MODE[value]
                            for value in options.allowed_channel_modes)
        key_func = operator.attrgetter("mode")
        iterator = mp3_file.iter_part_groups(key_func, part_type=AudioFrame)
        for mode, start, end in iterator:
            if mode not in allowed_modes:
                reporter.range_error(start, end, "forbidden channel mode %s",
                                     MODE_TO_LABEL[mode])

class SamplingRateRule (Task):
    @staticmethod
    def add_arguments(parser):
        parser.add_argument("--allowed-sampling-rate", metavar="RATE",
                            dest="allowed_sampling_rates", nargs="+",
                            type=int, default=[44100, 48000],
                            choices=(8000, 11025, 12000, 16000, 22050, 24000,
                                     32000, 44100, 48000),
                            help=("Permitted sample rates in Hz."
                                  "  Default: %(default)s"))

    @staticmethod
    def run(options, mp3_file, reporter):
        allowed_sampling_rates = options.allowed_sampling_rates
        key_func = operator.attrgetter("sampling_rate")
        iterator = mp3_file.iter_part_groups(key_func, part_type=AudioFrame)
        for sampling_rate, start, end in iterator:
            if sampling_rate not in allowed_sampling_rates:
                reporter.range_error(start, end, "forbidden sampling rate %sHz",
                                     sampling_rate)

class SameParametersRule (Task):
    @staticmethod
    def run(options, mp3_file, reporter):
        layers, modes, sampling_rates = set(), set(), set()
        for part in mp3_file.iter_parts(part_type=AudioFrame):
            layers.add(part.layer)
            modes.add(part.mode)
            sampling_rates.add(part.sampling_rate)
        if len(layers) > 1:
            # Is this valid?  I have no idea.  warning not error to be
            # safe.
            reporter.file_error("file uses multiple layers: %s",
                                ", ".join(layers))
        if len(modes) > 1:
            mode_labels = ", ".join(MODE_TO_LABEL[mode]
                                    for mode in modes)
            # This seems irregular too, but what do I know?
            reporter.file_warning("file uses multiple modes: %s", mode_labels)
        if len(sampling_rates) > 1:
            # This just doesn't seem at all right, so I make it an error.
            sampling_rates_str = ", ".join(sampling_rates)
            reporter.file_error("file uses multiple sampling rates: %s",
                                sampling_rates_str)

class ID3v2AllowedFramesRule (Task):
    _DEFAULT_REQUIRED_FRAMES = ("TIT2", "TPE1", "TRCK", "TALB",
                                "RVA2:album", "RVA2:track",
                                "TXXX:replaygain_album_gain",
                                "TXXX:replaygain_album_peak",
                                "TXXX:replaygain_track_gain",
                                "TXXX:replaygain_track_peak")
    _DEFAULT_ALLOWED_FRAMES = ("TCON", "TDRC", "TENC", "TLAN",
                               "TSSE", "TSOP", "TPUB", "APIC:",
                               "TXXX:ASIN",
                               "TXXX:AccurateRipDiscID",
                               "TXXX:AccurateRipResult",
                               "TXXX:Ripping tool",
                               "TXXX:Source") + _DEFAULT_REQUIRED_FRAMES

    @classmethod
    def add_arguments(cls, parser):
        parser.set_defaults(required_id3v2_frames=cls._DEFAULT_REQUIRED_FRAMES,
                            allowed_id3v2_frames=cls._DEFAULT_ALLOWED_FRAMES)
        parser.add_argument("--required-id3v2-frames",
                            nargs="+",
                            help="Require specific ID3v2 frames.",
                            metavar="FRAME")
        parser.add_argument("--no-required-id3v2-frames",
                            dest="required_id3v2_frames",
                            action="store_const", const=(),
                            help="Don't _require_ any ID3v2 frames.")
        parser.add_argument("--allowed-id3v2-frames",
                            nargs="+",
                            help="Allow specific ID3v2 frames.",
                            metavar="FRAME")

    @staticmethod
    def run(options, mp3_file, reporter):
        id3 = mp3_file.get_tag(Tag.ID3V2)
        if id3 is None or id3.version < (2, 0, 0):
            return
        present_frames = set(id3)
        required_frames = set(options.required_id3v2_frames)
        missing_required_frames = required_frames - present_frames
        if missing_required_frames:
            for frame in missing_required_frames:
                reporter.file_error("missing required ID3v2 frame %s", frame)
        forbidden_frames = (present_frames - required_frames
                            - set(options.allowed_id3v2_frames))
        try_repair = options.try_repair
        if try_repair:
            report_method = reporter.file_repaired
            message_prefix = "deleted "
        else:
            report_method = reporter.file_error
            message_prefix = ""
        for frame in forbidden_frames:
            if try_repair:
                del id3[frame]
            report_method("%sforbidden ID3v2 frame %s", message_prefix, frame)

ID3V1_FIELDS_TO_FRAMES = {
    "title": "TIT2",
    "artist": "TPE1",
    "album": "TALB",
    "year": "TDRC",
    "comment": "COMM",
    "track": "TRCK",
    "genre": "TCON",
    }
ID3V1_FRAMES_TO_FIELDS = dict((val, key) for key, val
                              in ID3V1_FIELDS_TO_FRAMES.iteritems())

class ID3v1RequiredFieldsRule (Task):
    _DEFAULT_REQUIRED_FIELDS = ("title", "artist", "album", "track")

    @classmethod
    def add_arguments(cls, parser):
        parser.add_argument("--required-id3v1-fields",
                            nargs="+",
                            choices=ID3V1_FIELDS_TO_FRAMES,
                            default=cls._DEFAULT_REQUIRED_FIELDS,
                            help=("Which fields of an ID3v1.1 tag are required."
                                  "  Choices: %(choices)s"
                                  "  Default: %(default)s"),
                            metavar="FIELD")
        parser.add_argument("--allow-id3v1-comment",
                            default=False, action="store_true",
                            help=("Allow the ID3v1 comment field to have a"
                                  " value."))

    @staticmethod
    def run(options, mp3_file, reporter):
        tag = mp3_file.get_tag(Tag.ID3V1)
        if not tag:
            return
        for field in options.required_id3v1_fields:
            frame = ID3V1_FIELDS_TO_FRAMES[field]
            if frame not in tag:
                reporter.file_error("ID3v1 tag missing %s field", field)
        if not options.allow_id3v1_comment and "COMM" in tag:
            if options.try_repair:
                del tag["COMM"]
                reporter.file_repaired("cleared ID3v1 comment field")
            else:
                reporter.file_error("ID3v1 comment field is set")

class APEv2AllowedItemsRule (Task):
    _REQUIRED_ITEMS = ()
    _ALLOWED_ITEMS = ("replaygain_album_gain", "replaygain_track_gain",
                      "replaygain_album_peak", "replaygain_track_peak",
                      "mp3gain_minmax", "mp3gain_album_minmax", "mp3gain_undo")

    @classmethod
    def add_arguments(cls, parser):
        parser.add_argument("--required-apev2-items",
                            nargs="+",
                            type=str.lower,
                            default=cls._REQUIRED_ITEMS,
                            help=("Required APEv2 tag items."
                                  " Default: %(default)s"),
                            metavar="ITEM")
        parser.add_argument("--allowed-apev2-items",
                            nargs="+",
                            type=str.lower,
                            default=cls._ALLOWED_ITEMS,
                            help=("Allowed (but not required) APEv2 tag items."
                                  " Default: %(default)s"),
                            metavar="ITEM")

    @staticmethod
    def run(options, mp3_file, reporter):
        tag = mp3_file.get_tag(Tag.APEV2)
        if not tag:
            return
        present_keys = set(key.lower() for key in tag)
        required_keys = set(key.lower() for key in options.required_apev2_items)
        for missing_key in (required_keys - present_keys):
            reporter.file_error("APEv2 tag missing %r item", missing_key)
        allowed_keys = set(key.lower() for key in options.allowed_apev2_items)
        forbidden_keys = present_keys - required_keys - allowed_keys
        try_repair = options.try_repair
        template = "forbidden APEv2 item %r"
        if try_repair:
            template = "deleting " + template
            report = reporter.file_repaired
        else:
            report = reporter.file_error
        for forbidden_key in forbidden_keys:
            if try_repair:
                del tag[forbidden_key]
            report(template, forbidden_key)

class CommitChanges (Task):
    @staticmethod
    def run(options, mp3_file, reporter):
        if options.try_repair:
            mp3_file.update_file()

COMMANDS = set()
add_command = COMMANDS.add

class CheckCommand (object):
    NAME = "check"

    TASKS = [
        InvalidDataRule,
        UnknownDataRule,
        ChannelModesRule,
        BitRateRule,
        SamplingRateRule,
        SameParametersRule,
        InvalidTagsRule,
        ExpectedTagsRule,
        CommitChanges,
        ID3v2AllowedFramesRule,
        ID3v1RequiredFieldsRule,
        APEv2AllowedItemsRule,
        CommitChanges,
        ]

    @classmethod
    def set_up_parser(cls, parser):
        parser.add_argument("--repair", "-r", dest="try_repair",
                            default=False, action="store_true",
                            help=('Try to "repair" problems in MP3s (usually'
                                  ' by deleting parts of the file)'))
        parser.add_argument("--min-bit-rate", type=int, default=161,
                            help=("Minimum average bit rate (kb/s) before a"
                                  " warning is emitted"))
        for task in cls.TASKS:
            task.add_arguments(parser)
        parser.add_argument("mp3_files", nargs="+")

    @classmethod
    def run(cls, options):
        for mp3_file_path in options.mp3_files:
            mp3_file = MP3File(mp3_file_path)
            reporter = Reporter(mp3_file)
            for task in cls.TASKS:
                task.run(options, mp3_file, reporter)

add_command(CheckCommand)

class InfoCommand (object):
    NAME = "info"

    @staticmethod
    def set_up_parser(parser):
        parser.add_argument("mp3_files", nargs="+")

    @staticmethod
    def run(options):
        for mp3_file_path in options.mp3_files:
            mp3_file = MP3File(mp3_file_path)
            kbit_rate = mp3_file.get_average_bit_rate() / 1000
            print "%s: average bit rate %dkb/s" % (mp3_file_path, kbit_rate)
            def key_func(part):
                if isinstance(part, Tag):
                    # We don't want Tags grouped together.
                    return id(part)
                else:
                    return (type(part), getattr(part, "error_messages", None))
            iterator = mp3_file.iter_part_groups(key_func)
            parts = mp3_file.parts
            for key, start, end in iterator:
                first_part, last_part = parts[start], parts[end - 1]
                length = last_part.end - first_part.start
                print "\t",
                if isinstance(first_part, Tag):
                    assert (end - start) == 1
                    tag = first_part
                    error_messages = tag.error_messages
                    if error_messages:
                        print "invalid ",
                    print "%s tag, %s byte(s)" % (tag.tag_type, length)
                else:
                    part_type, error_messages = key
                    if error_messages:
                        print "invalid ",
                    print "%s, %d part(s), %d byte(s)" % (part_type.__name__,
                                                          end - start, length)
                if error_messages:
                    for error_message in error_messages:
                        print "\t\t%s" % (error_message,)

add_command(InfoCommand)

class BriefCommand (object):
    NAME = "brief"

    @staticmethod
    def set_up_parser(parser):
        parser.add_argument("--tags", "-t", default=False, action="store_true",
                            help="Show tag info instead of audio info.")
        parser.add_argument("--width", "-w", type=int, default=0,
                            help=("Try to limit output lines to the given"
                                  " width."))
        parser.add_argument("mp3_files", nargs="+")

    @staticmethod
    def _show_audio_info(mp3_file, write):
        iterator = mp3_file.iter_parts(part_type=AudioFrame)
        try:
            exemplar = iterator.next()
        except StopIteration:
            exemplar = None
        else:
            layer = exemplar.layer
            mode = exemplar.mode
            sampling_rate = exemplar.sampling_rate
            bit_rate = exemplar.bit_rate
            cbr = True
            many_parameters = False
            for part in iterator:
                if part.bit_rate != bit_rate:
                    cbr = False
                if (part.layer != layer or part.mode != mode
                    or part.sampling_rate != sampling_rate):
                    many_parameters = True
        if exemplar is None:
            write("no audio")
        else:
            if cbr:
                write("CBR, %dkb/s" % (bit_rate / 1000,))
            else:
                write("VBR, %dkb/s average"
                      % (mp3_file.get_average_bit_rate() / 1000,))
            write(", ")
            if many_parameters:
                write("more than one set of parameters\n")
            else:
                write("layer %d, %s, %dHz" % (layer, MODE_TO_LABEL[mode],
                                              sampling_rate))

    @staticmethod
    def _show_tag_info(mp3_file, write):
        tags_present = [tag_type for tag_type
                        in (Tag.ID3V2, Tag.APEV2, Tag.ID3V1)
                        if mp3_file.get_tag(tag_type)]
        # get_tag doesn't support Lyrics3v2.
        try:
            candidate = mp3_file.parts[-2]
        except IndexError:
            pass
        else:
            if (isinstance(candidate, Tag)
                and candidate.tag_type == Tag.LYRICS3V2):
                tags_present.insert(-1, Tag.LYRICS3V2)
        write(", ".join(tags_present))

    @classmethod
    def run(cls, options):
        max_file_name_len = max(len(os.path.basename(name))
                                   for name in options.mp3_files)
        # 55 is the max length of our output plus one more column for
        # good measure.
        #
        # XXX cheap here, should make it better
        if options.width >= 56:
            max_file_name_len = min(max_file_name_len, options.width - 55)
        write = sys.stdout.write
        show_func = cls._show_tag_info if options.tags else cls._show_audio_info
        for mp3_file_path in options.mp3_files:
            mp3_file_name = os.path.basename(mp3_file_path)
            if len(mp3_file_name) > max_file_name_len:
                mp3_file_name = mp3_file_name[:max_file_name_len - 3] + "..."
            write("%-*s: " % (max_file_name_len, mp3_file_name))
            mp3_file = MP3File(mp3_file_path)
            show_func(mp3_file, write)
            write("\n")

add_command(BriefCommand)

class TagInfoCommand (object):
    NAME = "tag-info"

    @staticmethod
    def set_up_parser(parser):
        parser.add_argument("--values", "-v", dest="show_values",
                            default=False, action="store_true")
        parser.add_argument("mp3_files", nargs="+")

    @staticmethod
    def _print_info_for_id3v2(options, tag, indent, write):
        show_values = options.show_values
        for key, value in sorted(tag.iteritems(), key=operator.itemgetter(0)):
            doc = value.__doc__.split("\n", 1)[0].rstrip(".")
            write("%s%s: %s\n" % (indent, key, doc))
            if show_values and (key[0] in "TW" or key.startswith("COMM")
                                or key.startswith("RVA2")):
                write("%s= %s\n" % (indent, value))

    @staticmethod
    def _print_info_for_id3v1(options, tag, indent, write):
        frames_and_fields = [(frame, ID3V1_FRAMES_TO_FIELDS[frame].capitalize())
                             for frame in tag]
        frames_and_fields.sort(key=operator.itemgetter(1))
        show_values = options.show_values
        for frame, field in frames_and_fields:
            write(indent)
            write(field)
            if show_values:
                write("=%s" % (tag[frame],))
            write("\n")

    @staticmethod
    def _print_info_for_apev2(options, tag, indent, write):
        show_values = options.show_values
        for key in sorted(tag):
            write(indent)
            write(key)
            if show_values:
                value = tag[key]
                if value.kind == mutagen.apev2.TEXT:
                    write("=%s" % (value,))
                else:
                    write(" does not contain text")
            write("\n")

    @classmethod
    def run(cls, options):
        for mp3_file_path in options.mp3_files:
            print "%s:" % (mp3_file_path,)
            mp3_file = MP3File(mp3_file_path)
            for tag_type, printer in ((Tag.ID3V2, cls._print_info_for_id3v2),
                                      (Tag.APEV2, cls._print_info_for_apev2),
                                      (Tag.ID3V1, cls._print_info_for_id3v1)):
                print "    %s:" % (tag_type,)
                tag = mp3_file.get_tag(tag_type)
                if tag:
                    printer(options, tag, " " * 8, sys.stdout.write)
                else:
                    print "        No %s tag" % (tag_type,)

add_command(TagInfoCommand)

class EditCommand (object):
    NAME = "edit"

    _PART_TYPES = {
        "valid-audio": (AudioFrame, True, None),
        "invalid-audio": (AudioFrame, False, None),
        "valid-id3v2": (Tag, True, Tag.ID3V2),
        "invalid-id3v2": (Tag, False, Tag.ID3V2),
        "valid-apev2": (Tag, True, Tag.APEV2),
        "invalid-apev2": (Tag, False, Tag.APEV2),
        "valid-lyrics3v2": (Tag, True, Tag.LYRICS3V2),
        "invalid-lyrics3v2": (Tag, False, Tag.LYRICS3V2),
        "valid-id3v1": (Tag, True, Tag.ID3V1),
        "invalid-id3v1": (Tag, False, Tag.ID3V1),
        "unknown-data": (UnknownData, None, None),
        }

    @classmethod
    def set_up_parser(cls, parser):
        parser.add_argument("-r", "--remove", action="store_true",
                            default=False,
                            help=("Remove given types, keep everything else."
                                  "  Default: keep only given types"))
        parser.add_argument("-o", "--output", metavar="FILE", default=None,
                            help=("Output file."
                                  "  Default: replace existing file"))
        parser.add_argument("mp3_file")
        parser.add_argument("part_types", nargs="*",
                            choices=tuple(cls._PART_TYPES),
                            help=("What kinds of parts to keep (or remove)"
                                  " from the input file."))

    @classmethod
    def run(cls, options):
        if not options.remove:
            types_to_keep = set(cls._PART_TYPES[name]
                                for name in options.part_types)
        else:
            types_to_keep = set(value
                                for name, value in cls._PART_TYPES.iteritems()
                                if name not in options.part_types)
        mp3_file = MP3File(options.mp3_file)
        iterator = mp3_file.iter_parts()
        for part in iterator:
            part_type = (type(part), getattr(part, "valid", None),
                         getattr(part, "tag_type", None))
            if part_type not in types_to_keep:
                iterator.delete_last()
        mp3_file.update_file(options.output or options.mp3_file)

add_command(EditCommand)

def main(argv=None):
    _logging.basicConfig()
    if argv is None:
        argv = sys.argv
    parser = argparse.ArgumentParser(prog=argv[0])
    parser.add_argument("--debug", dest="log_level", default=_logging.INFO,
                        action="store_const", const=_logging.DEBUG)
    subparsers = parser.add_subparsers()
    for command in COMMANDS:
        command_parser = subparsers.add_parser(command.NAME)
        command.set_up_parser(command_parser)
        command_parser.set_defaults(_run_method=command.run)
    options = parser.parse_args(argv[1:])
    _logging.getLogger().setLevel(options.log_level)
    options._run_method(options)

if __name__ == "__main__":
    main(sys.argv)
