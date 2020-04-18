import hither2 as hi

@hi.function('to_file', '0.1.0')
def to_file(path):
    return hi.File(path)