/*
 * Filename : RunCmdOnMulThreads.cpp
 *
 * Author   : Abhishek Khaitan
 *
 * Usage    : RunCmdOnMulThreads.exe   run.bat   C:\Temp\*.log
 * 
 *            Multiple instances of batch file will be made depending on
 *            number of CPUs on the sytem.
 *
 *            Prepare a batch file and write commands to be executed in it.
 *            The batch file will be run with the filename as parameter.
 *            run.bat can be replaced by any executable.
 *
 */

#include "stdafx.h"
#include <iostream>
#include <iterator>
#include <vector>
#include <algorithm>
#include <string>
#include <set>
#include <map>
#include <thread>
#include <mutex>
#include <chrono>
#include <sstream>
#include <atomic>
#include <random>
#include <crtdbg.h>
#include <boost/filesystem.hpp>
#include <boost/regex.hpp>
#include <boost/algorithm/string/replace.hpp>

using namespace std;
using namespace boost::filesystem;
using namespace std::chrono;

namespace
{
  class ExecuteMulThreads
  {
    // shared vars
    mutex mutex_prot_shared_vars; // mutex to protect shared vars
    atomic<int> num_threads_cur_running;  // (shared var)
    int sh_num_files_already_processed; // (shared var - need mutex protection)
    vector <double> v_exec_dur; // execution time (shared var - need mutex protection)

    // mutex prot needed only if changed from within child threads
    vector <string> v_files_to_be_processed; // files that need processing (shared var - need mutex protection if want to add/del from thread)
    int num_files_to_be_processed; // total number of files to be processed (not to be changed)

    // execution time for spawning threads
    system_clock::time_point t_overall_exec_start_time;
    system_clock::time_point t_overall_exec_end_time;
    double d_overall_exec_duration;

    // create one thread per logical cpu. keeps count of logical cpus
    int max_num_threads;

    // for generating random sleep duration to test thread execution
    random_device rand_device;
    uniform_int_distribution<int> rand_dist;

    // from cmd line param: store folder from which we want to execute shell scrips
    string s_folder_name;

    // cmd to execute
    string s_cmd_to_execute;

    void do_actual_work(string inpfname, int filenum, int expected_thread_running_time = 0);
    void wait_for_running_threads(vector<thread> &v_threads);
    void spawn_threads(void);
    string wildcard_to_regex(const string & s_wildcard);

  public:
    void worker_thread_run_apex(int thread_num);
    void process_folder(_TCHAR* tc_cmd_to_execute, _TCHAR* tc_foldername);
    void show_thread_running_time(void);

    // constructor
    ExecuteMulThreads() : rand_dist(5, 9) // random number between 5 and 9
    {
      // no child thread is exeucting at the start
      num_threads_cur_running = 0;

      // support creation of one thread per logical cpu
      max_num_threads = thread::hardware_concurrency();
    }
  }; // class
}; // namespace

void ExecuteMulThreads::do_actual_work(string inpfname, int filenum, int expected_thread_running_time)
{
  string run_cmd;

  run_cmd = s_folder_name + "\\" + s_cmd_to_execute + " \"" + inpfname + "\" " + to_string(expected_thread_running_time);
  //cout << run_cmd << endl;

  // will block till cmd executes. 
  system(run_cmd.c_str());
}

void ExecuteMulThreads::worker_thread_run_apex(int thread_num)
{
  ostringstream cout_sstream;
  auto thread_id = this_thread::get_id();
  int sleep_dur;
  int rand_num;
  int file_idx;
  string fname;
  bool exit_loop = false;

  do
  {
    // check if there is any more file to be processed
    mutex_prot_shared_vars.lock();
    if (sh_num_files_already_processed < num_files_to_be_processed)
    {
      // note: post increment as we want value before increment has been done
      file_idx = sh_num_files_already_processed++;
    }
    else
    {
      exit_loop = true;
    }
    mutex_prot_shared_vars.unlock();

    // exit if no more files are present
    if (exit_loop)
    {
      break;
    }

    // num of threads actively processing
    num_threads_cur_running++;

    // find the file to be processed
    fname = v_files_to_be_processed[file_idx];

    rand_num = rand_dist(rand_device);
    sleep_dur = rand_num + 5;

    cout_sstream << thread_num << "|" << num_threads_cur_running << "|" << file_idx << ": START: " << thread_id << ":: " << fname << endl;
    cout << cout_sstream.str();
    cout_sstream.str("");

    // execution time: store start time
    auto exec_start_time = system_clock::now();

    do_actual_work(fname, file_idx, sleep_dur);

    // execution time: find execution time
    auto exec_end_time = system_clock::now();
    duration<double> exec_duration = exec_end_time - exec_start_time;
    double exec_dur = exec_duration.count();

    cout_sstream << thread_num << "|" << num_threads_cur_running << "|" << file_idx << ": END  : " << thread_id << ". Exec time: " << exec_dur << "s" << endl;
    cout << cout_sstream.str();
    cout_sstream.str("");

    // execution time: store execution time for each thread in an array
    mutex_prot_shared_vars.lock();
    v_exec_dur.push_back(exec_dur);
    mutex_prot_shared_vars.unlock();

    num_threads_cur_running--;

  } while (exit_loop == false);

}

void ExecuteMulThreads::wait_for_running_threads(vector<thread> &v_threads)
{
  ostringstream cout_sstream;
  int num_threads;

  num_threads = v_threads.size();

  // wait for those 8 threads to complete
  for (int i = 0; i < num_threads; i++)
  {
    /*
    cout_sstream << i << "|" << num_threads_cur_running << ": WAIT : " << v_threads[i].get_id() << endl;
    cout << cout_sstream.str();
    cout_sstream.str("");
    */
    v_threads[i].join();
  }
}

void ExecuteMulThreads::spawn_threads(void)
{
  vector<thread> v_threads;

  // execution time: start time
  t_overall_exec_start_time = system_clock::now();

  for (int i = 0; i < max_num_threads; i++)
  {
    //lambda fn needed to create thread from class member function
    v_threads.push_back(thread([=]{ worker_thread_run_apex(i); }));
  }

  // wait for all threads to finish execution
  wait_for_running_threads(v_threads);

  // execution time: end time
  t_overall_exec_end_time = system_clock::now();
  duration<double> t_overall_exec_duration = t_overall_exec_end_time - t_overall_exec_start_time;
  d_overall_exec_duration = t_overall_exec_duration.count();

  // print exec time per thread
  show_thread_running_time();

  // print tot exec time
  cout << "Total Execution time for " << num_files_to_be_processed << " files: " << d_overall_exec_duration << "s\n";

}

string ExecuteMulThreads::wildcard_to_regex(const string & s_inp_wildcard)
{
  string s_result(s_inp_wildcard);
  boost::replace_all(s_result, ".", "\\.");
  boost::replace_all(s_result, "*", ".*");
  boost::replace_all(s_result, "?", "(.{1,1})");

  return s_result;
}

string cvt_tchar_to_string(_TCHAR *tc_inp)
{
  wstring ws_temp(tc_inp);
  string s_result(ws_temp.begin(), ws_temp.end());

  return s_result;
}

void ExecuteMulThreads::process_folder(_TCHAR* tc_cmd_to_execute, _TCHAR* tc_foldername)
{
  // fill in v_files_to_be_processed
  path path_inpfolder = tc_foldername;
  auto dirname = path_inpfolder.branch_path();
  string s_wildcard_fname = path_inpfolder.filename().string();
  string s_regex_fname = wildcard_to_regex(s_wildcard_fname);
  boost::regex r_wildcard_fname(s_regex_fname);

  // store dir name and cmd to execute in global vars
  s_folder_name = dirname.string();
  s_cmd_to_execute = cvt_tchar_to_string(tc_cmd_to_execute);
  
  if (s_folder_name.length() == 0)
  {
    // make current directory if no folder name is present
    s_folder_name = ".\\";
    dirname = ".\\";
  }

  if (is_directory(s_folder_name))
  {
    for (directory_entry& x : directory_iterator(dirname))
    {
      auto & filefetched = x.path();
      string fname_with_path = filefetched.string();
      string fname = filefetched.filename().string();
      bool fname_match = boost::regex_match(fname, r_wildcard_fname);

      if (fname_match)
      {
        v_files_to_be_processed.push_back(fname_with_path);
      }
    }
  }

  // store the count of files to be processed
  num_files_to_be_processed = v_files_to_be_processed.size();

  // none of the files have been processed yet, so initialize to 0
  // mutex protection not needed as child threads have not been instantiated yet
  sh_num_files_already_processed = 0;

  spawn_threads();
}

void ExecuteMulThreads::show_thread_running_time(void)
{
  double sum_of_all_exec_times = 0;
  int thread_num = 0;

  for (auto exec_time_per_thread : v_exec_dur)
  {
    cout << thread_num << ": " << exec_time_per_thread << "s" << endl;

    sum_of_all_exec_times += exec_time_per_thread;
    thread_num++;
  }

  cout << "Sum of execution times for " << thread_num << " threads: " << sum_of_all_exec_times << endl;
}

void run_cmd_on_mul_threads(int argc, _TCHAR* argv[])
{
  if (argc != 3)
  {
    string s_exec_fname = cvt_tchar_to_string(argv[0]);
    cout << "Usage:\n" << s_exec_fname << " <cmd_to_run>   <files_to_be_processed>\n\n";
    cout << "Example:\n" << s_exec_fname << " run.bat  C:\\Temp\\*.log\n\n";
    return;
  }

  ExecuteMulThreads c;
  c.process_folder(argv[1], argv[2]);

}

int _tmain(int argc, _TCHAR* argv[])
{
  run_cmd_on_mul_threads(argc, argv);
	return 0;
}

