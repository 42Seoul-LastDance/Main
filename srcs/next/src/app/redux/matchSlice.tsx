import { PayloadAction, createSlice } from '@reduxjs/toolkit';
import { PlayerSide, GameJoinMode, GameMode } from '../enums';
import { CustomGameSet } from '../interfaces';

export interface MatchState {
  customSet: CustomGameSet;
  isMatched: boolean | undefined;
  isMatchInProgress: boolean;
  alreadyPlayed: boolean;
  side: PlayerSide;
  emoji: string;
  myEmoji: string;
  leftName: string;
  rightName: string;
}

const initialState: MatchState = {
  customSet: {
    joinMode: GameJoinMode.NONE,
    gameMode: GameMode.NONE,
    opponentName: '',
    opponentSlackId: '',
  },
  isMatched: undefined,
  isMatchInProgress: false,
  alreadyPlayed: false,
  side: PlayerSide.NONE,
  emoji: '',
  myEmoji: '',
  leftName: '???',
  rightName: '???',
};

export const matchSlice = createSlice({
  name: 'match',
  initialState,
  reducers: {
    setCustomSet: (
      state: MatchState,
      action: PayloadAction<{
        joinMode: GameJoinMode;
        gameMode: GameMode;
        opponentName: string | undefined;
        opponentSlackId: string | undefined;
      }>,
    ) => {
      state.customSet.joinMode = action.payload.joinMode;
      state.customSet.gameMode = action.payload.gameMode;
      state.customSet.opponentName = action.payload.opponentName;
      state.customSet.opponentSlackId = action.payload.opponentSlackId;
    },
    setIsMatched: (
      state: MatchState,
      action: PayloadAction<{ isMatched: boolean | undefined }>,
    ) => {
      state.isMatched = action.payload.isMatched;
    },
    setIsMatchInProgress: (
      state: MatchState,
      action: PayloadAction<{ isMatchInProgress: boolean }>,
    ) => {
      state.isMatchInProgress = action.payload.isMatchInProgress;
    },
    setSide: (
      state: MatchState,
      action: PayloadAction<{ side: PlayerSide }>,
    ) => {
      state.side = action.payload.side;
    },
    setEmoji: (state: MatchState, action: PayloadAction<{ emoji: string }>) => {
      state.emoji = action.payload.emoji;
    },
    setMyEmoji: (
      state: MatchState,
      action: PayloadAction<{ myEmoji: string }>,
    ) => {
      state.myEmoji = action.payload.myEmoji;
    },
    setNames: (
      state: MatchState,
      action: PayloadAction<{ leftName: string; rightName: string }>,
    ) => {
      state.leftName = action.payload.leftName;
      state.rightName = action.payload.rightName;
    },
	setAlreadyPlayed: (
		state: MatchState,
		action: PayloadAction<{ alreadyPlayed: boolean }>,
	) => {
		state.alreadyPlayed = action.payload.alreadyPlayed;
	},
  },
});

export default matchSlice.reducer;
export const {
  setIsMatched,
  setSide,
  setCustomSet,
  setEmoji,
  setMyEmoji,
  setNames,
  setIsMatchInProgress,
  setAlreadyPlayed,
} = matchSlice.actions;
