#!/usr/bin/env python
'''
  combine the project file and classpath created by "ligradle eclipse" into a top level project
  It can be used to update the .classpath of an existing project when new
dependencies are added  or create a
new project
  -. cd to the top level directory of a MP checkout
  -. run this script. It will first call "mint snapshot && ligrade eclipse"
  -. refresh existing project or create/import a new project in the top level dir

'''
import fnmatch
import os, shutil, time, sys, pdb, re
from optparse import OptionParser, OptionGroup

cur_dir = None  # current directory
project_file = None
classpath_file = None

# utility to compare version
def cmp_ver(version1, version2):
  def normalize(v):
    return [int(x) for x in re.sub(r'(\.0+)*$','', v).split(".")]
  return cmp(normalize(version1), normalize(version2))

def find_file_recursively(file_pattern):
  matches = []
  for root, dirnames, filenames in os.walk(cur_dir):
    for filename in fnmatch.filter(filenames, file_pattern):
      if os.popen("git ls-files %s" % root).read()  == "" and options.skip_non_git_file:
        print "WARNING: dir %s not in git. Skipping. Please consider rm the dir" % root
        continue
      matches.append(os.path.join(root, filename))
  return matches

def generate_project_file():
  project_file_content = '''<?xml version="1.0" encoding="UTF-8"?>
<projectDescription>
        <name>%s</name>
        <comment></comment>
        <projects>
        </projects>
        <buildSpec>
                <buildCommand>
                        <name>org.eclipse.jdt.core.javabuilder</name>
                        <arguments>
                        </arguments>
                </buildCommand>
        </buildSpec>
        <natures>
                <nature>org.eclipse.jdt.core.javanature</nature>
        </natures>
</projectDescription>
  ''' % os.path.basename(cur_dir)
  open(os.path.join(cur_dir,".project"),"w").writelines(project_file_content)

def backup_files():
  backup_suffix = time.strftime('%y%m%d_%H%M%S')
  if os.path.exists(project_file):
    shutil.copy(project_file, "%s_%s" % (project_file, backup_suffix))
  if os.path.exists(classpath_file):
    shutil.copy(classpath_file, "%s_%s" % (classpath_file, backup_suffix))

def dedup_lib_path(lib_path_dict):
  to_be_del_keys=[]
  lib_path_dict_dedup={}
  for key in lib_path_dict:
    m = re.search(' path="(.*)-([\d\.]+)',key)
    if m:
      lib_id = m.group(1).split("/")[-1]
      lib_ver = m.group(2).rstrip(".")
      if lib_id in lib_path_dict_dedup:
        save_ver = lib_path_dict_dedup[lib_id][0]
        if cmp_ver(save_ver,lib_ver) == -1: # need to replace it
          lib_path_dict_dedup[lib_id][0] = lib_ver
          to_be_del_keys.append(lib_path_dict_dedup[lib_id][1])
        else:
          to_be_del_keys.append(key)
      else:
         lib_path_dict_dedup[lib_id]=[lib_ver, key]
  for key in to_be_del_keys: 
    if key in lib_path_dict: del lib_path_dict[key]

def combine_class_path(classpath_files):
  source_path_dict = {}
  lib_path_dict = {}

  # read in the files, combine them
  re_src = re.compile('kind="src" path="([\w/]+)"')
  re_bogus_source_lib = re.compile(' path="(.+-sources.jar)"')
  re_build = re.compile("/build/")
  re_lib = re.compile('kind="lib"')
  re_classpath = re.compile("<classpathentry")
  re_no_close = re.compile("\">$")
  re_export = re.compile('exported="true"')
  #pdb.set_trace()
  for c_file in classpath_files:
    dirname = os.path.dirname(c_file).split("%s/" % cur_dir)[1]
    for line in open(c_file):
      line.rstrip("\n")
      if not re_classpath.search(line): continue
      if re_build.search(line): continue  # skip build dir  
      m = re_src.search(line)
      if m: 
        if not re_export.search(line):
          source_dir = os.path.join(dirname, m.group(1))
          if os.path.exists(source_dir):
            source_path_dict['  <classpathentry kind="src" path="%s"/>' % source_dir] = 1
          # just skip
          #else: print "source dir does not eixst: %s" % source_dir
        continue
      # todo get rid of the the duplicate jackson
      if re_lib.search(line) and re_bogus_source_lib.search(line):
        # Skip bogus "lib" dependencies that actually point to source jars. 
        # There are some entries that have 'kind="lib"' but point to a -sources.jar file. These can break de-duplication later on, since the real jar might be
        # treated as a duplicate of the -sources.jar
        continue;
      elif re_lib.search(line): 
        if re_no_close.search(line): line = re_no_close.sub('"/>',line)  # close the classpath if it is not closed
        lib_path_dict[line] = 1
  # dedup jackson
  dedup_lib_path(lib_path_dict)
  return (source_path_dict.keys(), lib_path_dict.keys())
 
def generate_classpath_file(classpath_files):
  source_paths, lib_paths =  combine_class_path(classpath_files)
  content = '''<?xml version="1.0" encoding="UTF-8"?>
<classpath>
  <classpathentry kind="con" path="org.eclipse.jdt.launching.JRE_CONTAINER"/>
  <classpathentry kind="output" path="eclipse_build"/>
  %s
  %s
</classpath>
''' % ("\n".join(sorted(source_paths)), "\n".join(sorted(lib_paths)))
  open(os.path.join(cur_dir,".classpath"),"w").writelines(content)

def main(argv):
  global cur_dir, project_file, classpath_file
  cur_dir = os.getcwd()

  global options
  parser = OptionParser(usage="usage: %prog [options]")
  parser.add_option("", "--skip_build", action="store_true", dest="skip_build", default = False, help="skip build")
  parser.add_option("", "--skip_non_git_file", action="store_true", dest="skip_non_git_file", default = False, help="skip non svn file")
  (options, args) = parser.parse_args()
  
  if not options.skip_build:
    if not os.path.exists(os.path.join(cur_dir,"build.gradle")):
      print "current directory does not have build.gradle! Use --skip_build to skip ligradle call"
      sys.exit(1)
    os.system("ligradle jar eclipse")
  classpath_files = find_file_recursively(".classpath")
  if not classpath_files:
    print "There is no .classpath under current directory!"
    sys.exit(1)
  # do the eclipse #call
  project_file = os.path.join(cur_dir,".project")
  classpath_file = os.path.join(cur_dir,".classpath")
  backup_files()
  generate_project_file()
  if classpath_file in classpath_files: classpath_files.remove(classpath_file)  # remove the current one
  generate_classpath_file(classpath_files)

if __name__ == "__main__":
    main(sys.argv[1:])

