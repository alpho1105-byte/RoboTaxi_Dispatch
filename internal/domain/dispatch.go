package domain

type Decision string

const (
	Accept Decision = "ACCEPT"
	Reject Decision = "REJECT"
)

// CommandKind 是服務端送給車輛的指令種類。
type CommandKind string

const (
	// CommandDispatch：詢問車輛是否願意接這趟行程。
	CommandDispatch CommandKind = "DISPATCH"
	// CommandConfirm：配對成立，車輛可以出發去接乘客。
	CommandConfirm CommandKind = "CONFIRM"
	// CommandCancel：撤回先前的派車詢問，例如行程已經被別台車接走。
	CommandCancel CommandKind = "CANCEL"
)

type Command struct {
	Kind        CommandKind
	RideID      string
	Pickup      Point
	Destination Point
}

// DispatchDecision 是車輛對派車詢問的回覆。
type DispatchDecision struct {
	RideID   string
	AVID     string
	Decision Decision
	Reason   string
}

type TripEventKind string

const (
	TripPickedUp  TripEventKind = "PICKED_UP"
	TripCompleted TripEventKind = "COMPLETED"
)
