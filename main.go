package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"

	"github.com/gorilla/websocket"
)

var wordList []string
var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

type GenericMessage struct {
	Type          string     `json:"type"`
	Grids         [][]string `json:"grids"`
	KnownSolution string     `json:"knownSolution,omitempty"`
}
type WebSocketMessage struct {
	Status        string   `json:"status,omitempty"`
	PossibleWords []string `json:"possibleWords,omitempty"`
	Error         string   `json:"error,omitempty"`
}
type knowledge struct {
	greens       [5]rune
	yellows      map[rune][]int
	letterCount  map[rune]int
	isCountExact map[rune]bool
}

func main() {
	if err := loadWordList("valid-wordle-words.txt"); err != nil {
		log.Fatalf("Failed to load word list: %v", err)
	}
	http.HandleFunc("/", serveFrontend)
	http.HandleFunc("/ws", handleConnections)
	fmt.Println("Wordle Detective server listening on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
func loadWordList(filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("Warning: '%s' not found. Using a small default wordlist.", filename)
			defaultWords := []string{"abide", "about", "above", "react", "salty", "sauce", "solve", "audio", "adieu", "trace", "roast", "grail", "atria", "speed", "eerie", "level", "poppy", "proxy", "chave", "shave"}
			wordList = defaultWords
			return os.WriteFile(filename, []byte(strings.Join(defaultWords, "\n")), 0644)
		}
		return err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		word := strings.ToLower(strings.TrimSpace(scanner.Text()))
		if len(word) == 5 {
			wordList = append(wordList, word)
		}
	}
	log.Printf("Loaded %d words from %s", len(wordList), filename)
	return scanner.Err()
}
func serveFrontend(w http.ResponseWriter, r *http.Request) { http.ServeFile(w, r, "index.html") }
func handleConnections(w http.ResponseWriter, r *http.Request) {
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("Upgrade error:", err)
		return
	}
	defer ws.Close()
	log.Println("Client connected via WebSocket")
	sendChan := make(chan WebSocketMessage, 10)
	go func() {
		defer ws.Close()
		for msg := range sendChan {
			payload, _ := json.Marshal(msg)
			if err := ws.WriteMessage(websocket.TextMessage, payload); err != nil {
				log.Println("WebSocket write error:", err)
				return
			}
		}
		ws.WriteMessage(websocket.CloseMessage, []byte{})
	}()
	for {
		_, msg, err := ws.ReadMessage()
		if err != nil {
			log.Println("WebSocket read error:", err)
			close(sendChan)
			break
		}
		var req GenericMessage
		if err := json.Unmarshal(msg, &req); err != nil {
			sendChan <- WebSocketMessage{Error: "Invalid request format"}
			continue
		}
		if len(req.Grids) == 0 {
			sendChan <- WebSocketMessage{Error: "No grids provided"}
			continue
		}
		switch req.Type {
		case "solve":
			go solveAndRespond(sendChan, req)
		case "verify":
			go handleVerification(sendChan, req)
		default:
			sendChan <- WebSocketMessage{Error: "Unknown request type"}
		}
	}
}
func handleVerification(sendChan chan<- WebSocketMessage, req GenericMessage) {
	word := strings.ToLower(req.KnownSolution)
	log.Printf("Verifying word: %s", word)
	var wg sync.WaitGroup
	verificationResults := make([]string, len(req.Grids))
	isValidOverall := true
	for i, grid := range req.Grids {
		wg.Add(1)
		go func(g []string, index int) {
			defer wg.Done()
			isValid, reason := isWordValidForGrid(word, g)
			if isValid {
				verificationResults[index] = fmt.Sprintf("Grid %d: ✅ VALID", index+1)
			} else {
				isValidOverall = false
				verificationResults[index] = fmt.Sprintf("Grid %d: ❌ INVALID (Reason: %s)", index+1, reason)
			}
		}(grid, i)
	}
	wg.Wait()
	finalStatus := fmt.Sprintf("Verification for '%s': %s", strings.ToUpper(word), strings.Join(verificationResults, " | "))
	if !isValidOverall {
		finalStatus += ". The word is not a possible solution."
	} else {
		finalStatus += ". The word is a valid solution for all grids."
	}
	sendChan <- WebSocketMessage{Status: finalStatus, PossibleWords: []string{}}
}
func solveAndRespond(sendChan chan<- WebSocketMessage, req GenericMessage) {
	sendChan <- WebSocketMessage{Status: fmt.Sprintf("Processing %d grid(s)...", len(req.Grids))}
	var wg sync.WaitGroup
	resultsChan := make(chan map[string]bool, len(req.Grids))
	for i, grid := range req.Grids {
		wg.Add(1)
		go func(g []string, gridNum int) {
			defer wg.Done()
			sendChan <- WebSocketMessage{Status: fmt.Sprintf("[Grid %d] Simulating game...", gridNum)}
			gridPossibles := solveGrid(g)
			resultsChan <- gridPossibles
		}(grid, i+1)
	}
	wg.Wait()
	close(resultsChan)
	sendChan <- WebSocketMessage{Status: "Intersecting results..."}
	var finalPossibleWords map[string]bool
	isFirstResult := true
	for resultSet := range resultsChan {
		if isFirstResult {
			finalPossibleWords = resultSet
			isFirstResult = false
		} else {
			for word := range finalPossibleWords {
				if !resultSet[word] {
					delete(finalPossibleWords, word)
				}
			}
		}
	}
	if finalPossibleWords == nil {
		finalPossibleWords = make(map[string]bool)
	}
	resultSlice := make([]string, 0, len(finalPossibleWords))
	for word := range finalPossibleWords {
		resultSlice = append(resultSlice, word)
	}
	sendChan <- WebSocketMessage{PossibleWords: resultSlice}
}

func solveGrid(grid []string) map[string]bool {
	possibleAnswers := make(map[string]bool)
	for _, word := range wordList {
		if isValid, _ := isWordValidForGrid(word, grid); isValid {
			possibleAnswers[word] = true
		}
	}
	return possibleAnswers
}

func isWordValidForGrid(potentialAnswer string, grid []string) (bool, string) {
	k := knowledge{
		yellows:      make(map[rune][]int),
		letterCount:  make(map[rune]int),
		isCountExact: make(map[rune]bool),
	}
	for i, pattern := range grid {
		foundValidGuessForPattern := false
		for _, guess := range wordList {
			if isGuessValidWithKnowledge(guess, &k) {
				if generatePattern(guess, potentialAnswer) == pattern {
					k.update(guess, pattern)
					foundValidGuessForPattern = true
					break
				}
			}
		}
		if !foundValidGuessForPattern {
			return false, fmt.Sprintf("at row %d (pattern '%s'), no valid guess found with current knowledge: %s", i+1, pattern, k.String())
		}
	}
	return true, ""
}

func (k *knowledge) String() string {
	var sb strings.Builder
	sb.WriteString("Greens: [")
	for i, r := range k.greens {
		if r == 0 {
			sb.WriteRune('_')
		} else {
			sb.WriteRune(r)
		}
		if i < 4 {
			sb.WriteRune(' ')
		}
	}
	sb.WriteString("], ")
	sb.WriteString("Counts: {")
	var countKeys []rune
	for r := range k.letterCount {
		countKeys = append(countKeys, r)
	}
	sort.Slice(countKeys, func(i, j int) bool { return countKeys[i] < countKeys[j] })
	first := true
	for _, r := range countKeys {
		if !first {
			sb.WriteString(", ")
		}
		countType := "min"
		if k.isCountExact[r] {
			countType = "exact"
		}
		sb.WriteString(fmt.Sprintf("%c:%d(%s)", r, k.letterCount[r], countType))
		first = false
	}
	sb.WriteString("}")
	return sb.String()
}

func (k *knowledge) update(guess string, pattern string) {
	guessRunes := []rune(guess)
	greenOrYellowCounts := make(map[rune]int)
	for i, p := range []rune(pattern) {
		if p == 'G' || p == 'Y' {
			greenOrYellowCounts[guessRunes[i]]++
		}
	}

	for letter, count := range greenOrYellowCounts {
		if count > k.letterCount[letter] {
			k.letterCount[letter] = count
		}
	}

	guessCounts := make(map[rune]int)
	for _, r := range guessRunes {
		guessCounts[r]++
	}

	for letter, countInGuess := range guessCounts {
		if countInGuess > greenOrYellowCounts[letter] {
			k.isCountExact[letter] = true
			k.letterCount[letter] = greenOrYellowCounts[letter]
		}
	}

	for i, p := range []rune(pattern) {
		letter := guessRunes[i]
		switch p {
		case 'G':
			k.greens[i] = letter
		case 'Y':
			isAlreadyThere := false
			for _, pos := range k.yellows[letter] {
				if pos == i {
					isAlreadyThere = true
					break
				}
			}
			if !isAlreadyThere {
				k.yellows[letter] = append(k.yellows[letter], i)
			}
		}
	}
}

func isGuessValidWithKnowledge(guess string, k *knowledge) bool {
	guessRunes := []rune(guess)
	guessCounts := make(map[rune]int)
	for _, r := range guessRunes {
		guessCounts[r]++
	}
	for i, greenLetter := range k.greens {
		if greenLetter != 0 && guessRunes[i] != greenLetter {
			return false
		}
	}
	for i, guessLetter := range guessRunes {
		if invalidPositions, ok := k.yellows[guessLetter]; ok {
			for _, pos := range invalidPositions {
				if i == pos {
					return false
				}
			}
		}
	}
	for letter, guessCount := range guessCounts {
		if k.isCountExact[letter] && guessCount != k.letterCount[letter] {
			return false
		}
	}
	return true
}

func generatePattern(guess, answer string) string {
	result := []rune{'B', 'B', 'B', 'B', 'B'}
	answerChars := []rune(answer)
	answerLetterCounts := make(map[rune]int)
	for _, r := range answerChars {
		answerLetterCounts[r]++
	}
	for i, guessRune := range []rune(guess) {
		if guessRune == answerChars[i] {
			result[i] = 'G'
			answerLetterCounts[guessRune]--
		}
	}
	for i, guessRune := range []rune(guess) {
		if result[i] != 'G' {
			if count, ok := answerLetterCounts[guessRune]; ok && count > 0 {
				result[i] = 'Y'
				answerLetterCounts[guessRune]--
			}
		}
	}
	return string(result)
}
