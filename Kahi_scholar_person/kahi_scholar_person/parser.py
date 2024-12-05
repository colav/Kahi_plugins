from kahi_impactu_utils.Utils import doi_processor, split_names, get_name_connector
from unidecode import unidecode
from time import time
import copy


def process_authors(s_work, verbose=0):
    authors = s_work["author"].lower().split(" and ")
    authors_work = []
    if s_work["profiles"]:
        num_profiles = len(s_work["profiles"])
        match_profiles = 0
        author_raw = None
        for profile_name, profile_id in s_work["profiles"].items():
            if match_profiles == num_profiles:
                break
            profile_name_ = [unidecode(part) for part in profile_name.replace(
                "-", " ").lower().split()]
            len_profile_name_ = len(profile_name_)
            # Names list
            author_found = False
            fixed_ln = None
            try:
                for author in authors:
                    if author_found:
                        break
                    name_parts = author.split(",")
                    if len(name_parts) <= 1:
                        continue  # Avoid elements that are not names
                    last_names, first_names = [
                        unidecode(part) for part in name_parts[0].replace("-", " ").split()], [
                        unidecode(part) for part in ", ".join(name_parts[1:]).replace("-", " ").split()]
                    raw_last_names, raw_first_names = [
                        part for part in name_parts[0].replace("-", " ").split()], [
                        part for part in ", ".join(name_parts[1:]).replace("-", " ").split()]
                    if len_profile_name_ >= 4:
                        part_mone, part_mtwo, part_one = profile_name_[
                            -1], profile_name_[-2], profile_name_[0]
                        if part_mone in last_names and part_mtwo in last_names and part_one[0] in [ln[0] for ln in first_names]:
                            author_found = True  # Set author_found to True
                            author_raw = author
                        else:
                            for ln in last_names:
                                if "\\" in ln:
                                    author_found, fixed_ln = backslash_in_last_names(
                                        ln, profile_name_, first_names, last_names, len_profile_name_)
                                    if author_found:
                                        author_raw = author
                    elif len_profile_name_ == 3:
                        part_one, part_two, part_three = profile_name_[
                            0], profile_name_[1], profile_name_[2]
                        if part_two in last_names and part_three in last_names and part_one[0] in [ln[0] for ln in first_names]:
                            author_found = True  # Set author_found to True
                            author_raw = author
                        else:
                            for ln in last_names:
                                if "\\" in ln:
                                    author_found, fixed_ln = backslash_in_last_names(
                                        ln, profile_name_, first_names, last_names, len_profile_name_)
                                    if author_found:
                                        author_raw = author
                    elif len_profile_name_ == 2:
                        part_one, part_two = profile_name_[0], profile_name_[1]
                        if part_two in last_names and part_one[0] in [ln[0] for ln in first_names]:
                            author_found = True  # Set author_found to True
                            author_raw = author
                        else:
                            for ln in last_names:
                                if "\\" in ln:
                                    author_found = backslash_in_last_names(
                                        ln, profile_name_, first_names, last_names, len_profile_name_)
                                    if author_found:
                                        author_raw = author
            except Exception as e:
                if verbose > 4:
                    print(e)
                pass

            if author_found and fixed_ln:
                full_name = " ".join(elm.strip().capitalize() for elm in raw_first_names) + " " + " ".join(
                    elm.strip().capitalize().replace(ln, fixed_ln) for elm in last_names)
                authors_work.append({"full_name": " ".join(full_name.replace(".", " ").split()), "author": author_raw,
                                    "alias": profile_name, "scholar_id": profile_id})
                match_profiles += 1
            elif author_found:
                full_name = " ".join(elm.strip().capitalize() for elm in raw_first_names) + \
                    " " + " ".join(elm.strip().capitalize()
                                   for elm in raw_last_names)
                authors_work.append({"full_name": " ".join(full_name.replace(".", " ").split()), "author": author_raw,
                                    "alias": profile_name, "scholar_id": profile_id})
                match_profiles += 1
            if author_raw in authors:
                authors.remove(author_raw)

    for author in authors:
        name_parts = author.split(",")
        raw_last_names, raw_first_names = [
            part for part in name_parts[0].replace("-", " ").split()], [
            part for part in ", ".join(name_parts[1:]).replace("-", " ").split()]
        full_name = " ".join(elm.strip().capitalize() for elm in raw_first_names) + \
            " " + " ".join(elm.strip().capitalize() for elm in raw_last_names)
        authors_work.append(
            {"full_name": " ".join(full_name.replace(".", " ").split()), "author": author, "alias": "", "scholar_id": ""})
    return authors_work


def backslash_in_last_names(ln, profile_name_, first_names, last_names, len_profile_name_):
    last_names_i = last_names.copy()
    for vocal in ["a", "e", "i", "o", "u"]:
        for i, ln in enumerate(last_names_i):
            if "\\" in ln:
                last_names[i] = ln.replace("\\", vocal)
            if len_profile_name_ >= 4:
                part_mone, part_mtwo, part_one = profile_name_[
                    -1], profile_name_[-2], profile_name_[0]
                if part_mone in last_names and part_mtwo in last_names and part_one[0] in [ln[0] for ln in first_names]:
                    return True, last_names[i]
            elif len_profile_name_ == 3:
                part_one, part_two, part_three = profile_name_[
                    0], profile_name_[1], profile_name_[2]
                if part_two in last_names and part_three in last_names and part_one[0] in [ln[0] for ln in first_names]:
                    return True, last_names[i]
            elif len_profile_name_ == 2:
                part_one, part_two = profile_name_[0], profile_name_[1]
                if part_two in last_names and part_one[0] in [ln[0] for ln in first_names]:
                    return True, last_names[i]
    return False, None


def parse_scholar(reg, empty_person, verbose=0):
    entries = []
    authors_work = process_authors(reg, verbose=0)
    if authors_work:
        for author in authors_work:
            if author["full_name"] == 'others':
                continue
            entry = copy.deepcopy(empty_person)
            entry["updated"].append({"source": "scholar", "time": int(time())})
            entry["full_name"] = author["full_name"]
            if author["author"]:
                # chaking if the names has connector, more that 4 terms or if it doest have simcolon
                name_parts = author["full_name"].split()
                connectors = [e.title() for e in get_name_connector()]
                if len(name_parts) >= 4 or set(connectors).intersection(name_parts) or len(author["author"].split(",")) == 1:
                    # if so then just use split_name
                    author_data = split_names(author["full_name"])
                    entry["last_names"] = author_data["last_names"]
                    entry["first_names"] = author_data["first_names"]
                    entry["initials"] = author_data["initials"]
                else:
                    name_parts = author["author"].split(",")
                    if len(name_parts) > 1:
                        entry["last_names"] = name_parts[0].replace(
                            "-", " ").title().split()
                        entry["first_names"] = name_parts[1].replace(
                            "-", " ").title().split()
                        entry["initials"] = ''.join(
                            [i[0] for i in entry["first_names"]])
                entry["aliases"].append(author["author"])
            if author["alias"]:
                entry["aliases"].append(author["alias"])
            if author["scholar_id"]:
                entry["external_ids"].append({"provenance": "scholar", "source": "scholar",
                                             "id": "https://scholar.google.com/citations?user=" + author["scholar_id"]})
            if reg["cid"]:
                entry["related_works"].append(
                    {"provenance": "scholar", "source": "cid", "id": reg["cid"]})
            if reg["doi"]:
                doi = doi_processor(reg["doi"])
                if doi:
                    entry["related_works"].append(
                        {"provenance": "scholar", "source": "doi", "id": doi})

            entries.append(entry)
    return entries
