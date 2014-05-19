import comm_pb2
import struct
import sys
import socket
import time
import json


def serialize_and_send(request):
	#Serialize Protobuf
	ip = raw_input("Enter the ip address server")
	port = int(raw_input("Enter Port no:"))
	s = request.SerializeToString()
	packed_len = struct.pack('>L', len(s))

	#Socket Connection
	sock=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
	server_address=(ip, port)
	print('connecting to %s port %s' %server_address)
	sock.connect(server_address)
	print 'sending message'
	sock.sendall(packed_len + s)

	# Receive at s, a message of type Request and close connection
	return get_message(sock, comm_pb2.Request)
	sock.close()


#Request from the User
def handle_signUp():
	
	f_name = raw_input("Enter Name: ")
	user_name = raw_input("User name: ")
	password = raw_input("Enter Password: ")

	#Build for SignUp 
	request = comm_pb2.Request()
	request.header.routing_id = request.header.JOBS
	request.header.originator = "Client"
	request.header.tag = "SignUp"

	request.body.sign_up.full_name = f_name
	request.body.sign_up.user_name = user_name
	request.body.sign_up.password = password
	
	response = serialize_and_send(request)
	print response

def handle_signin():
	user_name = raw_input("Enter Email ID: ")
	password = raw_input("Enter Password: ")

	request = comm_pb2.Request()
	request.header.routing_id = request.header.JOBS
	request.header.originator = "Client"
	request.header.tag = "SignIn"

	request.body.sign_in.user_name = user_name
	request.body.sign_in.password 
	
	response = serialize_and_send(request)
	print response

def handle_list():
	#Build for ListFiles
	request = comm_pb2.Request()
	#Header
	request.header.routing_id = request.header.JOBS
	request.header.originator = "client"
	request.header.tag = "CourseList"
	#Payload
	#for CourseList in request.body.req_list. 
	get_course = comm_pb2.GetCourse()
	get_course.course_id = -1
	get_course.course_name = ""
	get_course.course_description = ""
	print "adding course"
	request.body.req_list.CourseList.add()
	request.body.req_list.CourseList[0].course_id = -1
	print "added course"
	#request.body.req_list.CourseList.course_name = ""
	#request.body.req_list.CourseList.course_description = ""
	
	response = serialize_and_send(request)
	print "Job status"
	print request.body.job_status

	#Parsing Protobuf File
	total_courses = len(response.body.req_list.CourseList)
	print (total_courses) 
	courses = comm_pb2.GetCourse()
	print "List Of Course ::"
	print "##################################"
	for courses in response.body.req_list.CourseList:
	#for i in range(0,total_courses)	
		print courses
           
	print "##################################"


#Handle Request for Desc
def handle_getCourse():
	#Build for Get Desc
	course_value = int(raw_input("Enter Course No for which Desc has to be received : "))

	request = comm_pb2.Request()
	#Header
	request.header.routing_id = request.header.JOBS
	request.header.originator  = "Client"
	request.header.tag = "SearchCourse"
	#Payload
	request.body.get_course.course_id = course_value
	
	#Serialize Data
	response = serialize_and_send(request)

	print "##################################"
	print course_value + "::" + response.body.get_course.course_name + "::" + response.body.get_course.course_description
	print "##################################"

def handle_ping():
	#Build for Get Desc
	import time
	request = comm_pb2.Request()
	#Header
	request.header.routing_id = request.header.PING
	request.header.originator  = "Client"
	request.header.tag = "Ping"
	request.header.time = int(round(time.time()*1000))
	#Payload
	request.body.ping.number = 1
	request.body.ping.tag = "Hahahaha"
		
	#Serialize Data
	response = serialize_and_send(request)

	print "##################################"
	print "Ping from server: " + response.SerializeToString()
	print "##################################"

def handle_voting():
	request = comm_pb2.Request()
	#Header
	request.header.routing_id = request.header.JOBS
	request.header.originator  = "Client"
	request.header.tag = "Voting"
	#Payload
	
	request.body.init_voting.voting_id = "1"
	
		
	#Serialize Data
	response = serialize_and_send(request)

	print "##################################"
	print "Ping from server: " + response.SerializeToString()
	print "##################################"


def get_message(sock, msgtype):
    """ Read a message from a socket. msgtype is a subclass of
        of protobuf Message.
    """
    len_buf = socket_read_n(sock, 4)
    msg_len = struct.unpack('>L', len_buf)[0]
    msg_buf = socket_read_n(sock, msg_len)

    msg = msgtype()
    msg.ParseFromString(msg_buf)
    return msg

def socket_read_n(sock, n):
    """ Read exactly n bytes from the socket.
        Raise RuntimeError if the connection closed before
        n bytes were read.
    """
    buf = ''
    while n > 0:
        data = sock.recv(n)
        if data == '':
            raise RuntimeError('unexpected connection close')
        buf += data
        n -= len(data)
    return buf


#User selection Option
# Main
def main():
    rerun = True
    while rerun:
	    selection = raw_input("\n 1.SignUp 2.SignIn 3.List 4.Get Course 5. Exit 6. Ping 7. Voting\n")
	    if selection == '1':
		handle_signUp()
	    elif selection == '2':
		handle_signin()
	    elif selection == '3':
		handle_list()
	    elif selection == '4':
		handle_getCourse()
            elif selection == '5':
		print "Bye"
		rerun = False
	    elif selection == '6':
		handle_ping()
	    elif selection == '7':
		handle_voting()
	    else : 
		print "Enter Valid Input"

if __name__ == '__main__':
    main()
