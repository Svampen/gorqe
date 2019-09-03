package main

import (
	"crypto/tls"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/jedib0t/go-pretty/list"
	"github.com/jedib0t/go-pretty/table"
	"github.com/jedib0t/go-pretty/text"
	"gopkg.in/urfave/cli.v2"

	pb "github.com/Svampen/gorqe/proto"
	proto "github.com/golang/protobuf/proto"
)

func init() {
	app := &cli.App{
		Name:  "gorqe",
		Usage: "CLI for RQE",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "host",
				Value:       "localhost",
				Usage:       "RQE `hostname` or ip",
				DefaultText: "localhost",
			},
			&cli.StringFlag{
				Name:        "port",
				Value:       "8322",
				Usage:       "RQE `port`",
				DefaultText: "8322",
			},
			&cli.BoolFlag{
				Name:        "nocolor",
				Value:       false,
				Usage:       "Disable colorized text",
				DefaultText: "false",
			},
		},
		Commands: []*cli.Command{
			{
				Name:    "cluster",
				Aliases: []string{"c"},
				Usage:   "cluster commands",
				Subcommands: []*cli.Command{
					&cli.Command{
						Name:    "status",
						Aliases: []string{"s"},
						Usage:   "get status for cluster",
						Action:  clusterStatus,
					},
				},
			},
			{
				Name:    "RQ",
				Aliases: []string{"rq"},
				Usage:   "RQ commands",
				Subcommands: []*cli.Command{
					{
						Name:    "add",
						Aliases: []string{"a"},
						Usage:   "add rq",
						Action:  addRQ,
					},
					{
						Name:      "delete",
						Aliases:   []string{"d"},
						Usage:     "delete <rq uuid>",
						UsageText: "will not verify if the rq exists or not before deleting. Will be successful even if it doesn't exist",
						Action:    deleteRQ,
					},
				},
			},
			{
				Name:    "entry",
				Aliases: []string{"e"},
				Usage:   "Entry commands",
				Subcommands: []*cli.Command{
					{
						Name:      "match",
						Aliases:   []string{"m"},
						Usage:     "match <entry>",
						UsageText: "send a request to RQE to match your <entry> with RQs",
						Action:    matchEntry,
						Flags: []cli.Flag{
							&cli.IntFlag{
								Name:        "entry_timeout",
								Value:       10000,
								Usage:       "`timeout` for entry matching",
								DefaultText: "10000",
								Aliases:     []string{"et"},
							},
						},
					},
				},
			},
		},
	}

	app.Run(os.Args)
}

func main() {

	log.SetFlags(log.Lshortfile)

}

func clusterStatus(c *cli.Context) error {

	request := &pb.Request{
		Msg: &pb.Request_StatusRequest{
			StatusRequest: &pb.StatusRequest{},
		},
	}

	response, err := sendReceiveMessage(c, request)
	if err != nil || response == nil {
		log.Println(err)
		cli.Exit("error", 10)
	}

	switch response.GetMsg().(type) {
	case *pb.Response_StatusResponse:
		parseClusterStatusResponse(response.GetStatusResponse())
	default:
		return cli.Exit("wrong responses message returned from RQE", 5)
	}
	return nil
}

func parseClusterStatusResponse(statusReponse *pb.StatusResponse) {
	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.AppendHeader(table.Row{"#", "Node Name", "Status", "RQ Count"})
	nodeInfos := statusReponse.GetNodeInfo()
	var totalRqCount int32 = 0
	for index, nodeInfo := range nodeInfos {
		name := nodeInfo.GetName()
		status := nodeInfo.GetNodeStatus().String()
		rqCount := nodeInfo.GetRqCount()
		totalRqCount += rqCount
		t.AppendRow([]interface{}{index, name, status, rqCount})
	}
	t.AppendFooter(table.Row{"", "", "Total", totalRqCount})
	t.Render()
}

func parseAddRQResponse(addRQResponse *pb.AddRQResponse, c *cli.Context) {
	status := addRQResponse.GetResponseStatus().GetStatus()
	reason := addRQResponse.GetResponseStatus().GetReason()
	uuid := addRQResponse.GetUuid()
	if status == pb.ResponseStatus_OK {
		if !c.Bool("nocolor") {
			fmt.Printf(text.Colors{text.FgGreen}.Sprint("Status ", status.String(), " uuid ", uuid, "\n"))
		} else {
			fmt.Print("Status ", status.String(), " uuid ", uuid, "\n")
		}
	} else {
		if !c.Bool("nocolor") {
			fmt.Printf(text.Colors{text.FgGreen}.Sprint("Status ", status.String(), " reason ", reason, "\n"))
		} else {
			fmt.Print("Status ", status.String(), " reason ", reason, "\n")
		}
	}
}

func parseDeleteRQResponse(deleteRQResponse *pb.DeleteRQResponse, c *cli.Context) {
	status := deleteRQResponse.GetResponseStatus().GetStatus()
	reason := deleteRQResponse.GetResponseStatus().GetReason()
	if status == pb.ResponseStatus_OK {
		if !c.Bool("nocolor") {
			fmt.Printf(text.Colors{text.FgGreen}.Sprint("Status ", status.String(), "\n"))
		} else {
			fmt.Print("Status ", status.String(), "\n")
		}
	} else {
		if !c.Bool("nocolor") {
			fmt.Printf(text.Colors{text.FgGreen}.Sprint("Status ", status.String(), " reason ", reason, "\n"))
		} else {
			fmt.Print("Status ", status.String(), " reason ", reason, "\n")
		}
	}
}

func parseMatchEntryReponse(matchEntryResponse *pb.MatchEntryResponse, c *cli.Context) {
	l := list.NewWriter()
	status := matchEntryResponse.GetResponseStatus().GetStatus()
	reason := matchEntryResponse.GetResponseStatus().GetReason()
	if status == pb.ResponseStatus_OK {
		rqs := matchEntryResponse.GetRqs()
		for _, rq := range rqs {
			uuid := rq.GetUuid()
			l.AppendItem(uuid)
			items := rq.GetRqItems()
			for _, item := range items {
				parseRQItem(item, l)
			}
		}
		if !c.Bool("nocolor") {
			for _, line := range strings.Split(l.Render(), "\n") {
				fmt.Print(text.Colors{text.FgGreen}.Sprintf("%s\n", line))
			}
		} else {
			for _, line := range strings.Split(l.Render(), "\n") {
				fmt.Printf("%s\n", line)
			}
		}
	} else {
		if !c.Bool("nocolor") {
			fmt.Printf(text.Colors{text.FgGreen}.Sprint("Status ", status.String(), " reason ", reason, "\n"))
		} else {
			fmt.Print("Status ", status.String(), " reason ", reason, "\n")
		}
	}
}

func parseRQItem(item *pb.RQItem, l list.Writer) {
	l.Indent()
	key := item.GetKey()
	l.AppendItem(key)
	l.Indent()
	switch item.GetValue().(type) {
	case *pb.RQItem_Rq:
		for _, i := range item.GetRq().GetRqItems() {
			parseRQItem(i, l)
		}
	case *pb.RQItem_Boolean:
		value := item.GetBoolean()
		l.AppendItem(value)
	case *pb.RQItem_Integer:
		value := item.GetInteger()
		l.AppendItem(value)
	case *pb.RQItem_String_:
		value := item.GetString_()
		l.AppendItem(value)
	}
	typeOptions := item.GetTypeOptions()
	operator := typeOptions.GetOperator()
	l.AppendItem(operator.String())
	if operator == pb.RQItemTypeOptions_COUNT {
		l.AppendItem(typeOptions.GetCount())
	}
	l.UnIndent()
	l.UnIndent()
}

func addRQ(c *cli.Context) error {
	rqItems := []*pb.RQItem{buildRQItem()}
	rq := pb.RQ{
		RqItems: rqItems,
	}
	rqReq := pb.AddRQRequest{
		Rq: &rq,
	}
	req := &pb.Request{
		Msg: &pb.Request_AddRqRequest{
			AddRqRequest: &rqReq,
		},
	}
	response, err := sendReceiveMessage(c, req)
	if err != nil || response == nil {
		log.Println(err)
		cli.Exit("error", 10)
	}

	switch response.GetMsg().(type) {
	case *pb.Response_AddRqResponse:
		parseAddRQResponse(response.GetAddRqResponse(), c)
		return nil
	default:
		return cli.Exit("wrong responses message returned from RQE", 5)
	}
}

func deleteRQ(c *cli.Context) error {
	if c.NArg() == 0 {
		return cli.Exit("Missing uuid argument", 12)
	}
	uuid := c.Args().First()
	fmt.Print("uuid to remove: ", uuid, "\n")
	req := &pb.Request{
		Msg: &pb.Request_DeleteRqRequest{
			DeleteRqRequest: &pb.DeleteRQRequest{
				Uuid: uuid,
			},
		},
	}
	response, err := sendReceiveMessage(c, req)
	if err != nil || response == nil {
		log.Println(err)
		cli.Exit("error", 10)
	}

	switch response.GetMsg().(type) {
	case *pb.Response_DeleteRqResponse:
		parseDeleteRQResponse(response.GetDeleteRqResponse(), c)
		return nil
	default:
		return cli.Exit("wrong responses message returned from RQE", 5)
	}
}

func matchEntry(c *cli.Context) error {
	if c.NArg() == 0 {
		return cli.Exit("Missing entry argument", 12)
	}
	var entry map[string]*pb.MatchEntryRequest_EntryValue = make(map[string]*pb.MatchEntryRequest_EntryValue)
	for _, kv := range c.Args().Slice() {
		keyvalue := strings.Split(kv, "=")
		if len(keyvalue) != 2 {
			return cli.Exit("Entry part "+kv+" not in correct format", 13)
		}
		buildEntry(entry, keyvalue[0], keyvalue[1])
	}
	req := &pb.Request{
		Msg: &pb.Request_MatchEntryRequest{
			MatchEntryRequest: &pb.MatchEntryRequest{
				Entry:   entry,
				Timeout: int32(c.Int("entry_timeout")),
			},
		},
	}

	response, err := sendReceiveMessage(c, req)
	if err != nil || response == nil {
		log.Println(err)
		cli.Exit("error", 10)
	}

	switch response.GetMsg().(type) {
	case *pb.Response_MatchEntryResponse:
		parseMatchEntryReponse(response.GetMatchEntryResponse(), c)
		return nil
	default:
		return cli.Exit("wrong responses message returned from RQE", 5)
	}
}

func buildEntry(entry map[string]*pb.MatchEntryRequest_EntryValue, key string, value string) {
	entry[key] = &pb.MatchEntryRequest_EntryValue{
		Value: &pb.MatchEntryRequest_EntryValue_String_{
			String_: value,
		},
	}
	b, err := strconv.ParseBool(value)
	if err == nil {

		entry[key] = &pb.MatchEntryRequest_EntryValue{
			Value: &pb.MatchEntryRequest_EntryValue_Boolean{
				Boolean: b,
			},
		}
	}
	i, err := strconv.ParseInt(value, 10, 32)
	if err == nil {
		entry[key] = &pb.MatchEntryRequest_EntryValue{
			Value: &pb.MatchEntryRequest_EntryValue_Integer{
				Integer: int32(i),
			},
		}
	}
}

func buildRQItem() *pb.RQItem {
	typeOptions := pb.RQItemTypeOptions{
		Operator: pb.RQItemTypeOptions_GT,
	}
	value := pb.RQItem_Integer{
		Integer: 10,
	}
	rqItem := pb.RQItem{
		Key:         "testing",
		Value:       &value,
		TypeOptions: &typeOptions,
	}
	return &rqItem
}

func sendReceiveMessage(c *cli.Context, request *pb.Request) (*pb.Response, error) {
	conf := &tls.Config{
		InsecureSkipVerify: true,
	}

	connection := c.String("host") + ":" + c.String("port")
	conn, err := tls.Dial("tcp", connection, conf)
	if err != nil {
		log.Println(err)
		return nil, cli.Exit(err, 10)
	}
	defer conn.Close()

	out, err := proto.Marshal(request)
	if err != nil {
		return nil, cli.Exit(err, 9)
	}

	n, err := conn.Write(out)
	if err != nil {
		log.Println(n, err)
		return nil, cli.Exit(err, 8)
	}

	buf := make([]byte, 4096)
	n, err = conn.Read(buf)
	if err != nil {
		log.Println(n, err)
		return nil, cli.Exit(err, 7)
	}

	response := &pb.Response{}
	err = proto.Unmarshal(buf[:n], response)
	if err != nil {
		return nil, cli.Exit(err, 6)
	}

	return response, nil
}
