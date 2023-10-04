import {
    BadRequestException,
    Injectable,
    InternalServerErrorException,
} from '@nestjs/common';
import { Socket } from 'socket.io';
import {
    GameMode,
    GameType,
    PlayerSide,
    GameStatus,
    GameEndStatus,
    Emoji,
} from './game.enum';
import {
    MIN,
    MAX,
    MINF,
    MAXF,
    MAXSCORE,
    TIMEZONE,
    BALL_SPEED,
    BALL_POS_X_MIN,
    BALL_POS_X_MAX,
    BALL_POS_Y_MIN,
    BALL_POS_Y_MAX,
    BALL_POS_Z_MIN,
    BALL_POS_Z_MAX,
    BALL_SCALE_X,
    BALL_SCALE_Y,
    BALL_SCALE_Z,
    PADDLE_SPEED,
    PADDLE_SCALE_X,
    PADDLE_SCALE_Y,
    PADDLE_SCALE_Z,
    PADDLE_POS_X,
    PADDLE_POS_Y,
    PADDLE_POS_Z_MIN,
    PADDLE_POS_Z_MAX,
    PADDLE_ROTATE_X,
    PADDLE_ROTATE_Y,
    PADDLE_ROTATE_Z,
    PADDING,
} from './game.constants';
import { GameRoom, Player } from './game.interface';
import { Game } from './game.entity';
import { InjectRepository } from '@nestjs/typeorm';
import { GameRepository } from './game.repository';
import { DateTime } from 'luxon';

@Injectable()
export class GameService {
    constructor(
        @InjectRepository(Game)
        private gameRepository: GameRepository,
    ) {}

    //Players
    private playerList: Map<string, Player> = new Map<string, Player>(); //socket.id
    //Queue
    private matchQueue: [string, string] = [undefined, undefined]; //socket.id
    private friendGameList: Map<number, string> = new Map<number, string>(); //userId, socket.id
    //Room
    private gameRoomIdx: number = 0;
    private gameRoomList: Map<number, GameRoom> = new Map<number, GameRoom>(); //room.id

    //PlayerList에 등록
    createPlayer(playerSocket: Socket) {
        const player: Player = {
            socket: playerSocket,
        };
        this.playerList.set(playerSocket.id, player);
    }

    //disconnect 시 처리
    async handleDisconnect(playerId: string) {
        const player = this.playerList.get(playerId);
        if (player.userId === undefined) return;
        if (player.roomId === undefined) {
            // in Queue => queue에서 제거
            if (player.gameType === GameType.MATCH) {
                if (this.matchQueue[player.gameMode] === player.socket.id)
                    this.matchQueue[player.gameMode] = undefined;
                else throw new BadRequestException('player not in the queue');
            } else if (player.gameType === GameType.FRIEND) {
                if (this.friendGameList.get(player.userId)) {
                    this.friendGameList.delete(player.userId);
                } else throw new BadRequestException('friend: bad request');
            }
        } else {
            //in room (waiting or game)
            const gameRoom = this.gameRoomList.get(player.roomId);
            if (gameRoom.gameStatus === GameStatus.WAIT) {
                //in waiting Room => 상대방에게 대기방 나가기 이벤트 발생!
                const rival: Player = this.playerList.get(
                    gameRoom.socket[(player.side + 1) % 2].id,
                );
                this.resetPlayer(rival);
                rival.socket.emit('kickout');
            } else if (gameRoom.gameStatus === GameStatus.GAME) {
                //게임중 => 게임 강제종료
                await this.finishGame(
                    gameRoom.id,
                    (player.side + 1) % 2,
                    GameEndStatus.DISCONNECT,
                );
            } else throw new BadRequestException('무슨 에러지..?');
            //gameroom 없애기
            this.gameRoomList.delete(player.roomId);
        }
    }
    //Player 삭제 (handleDisconnect용)
    deletePlayer(playerId: string) {
        this.playerList.delete(playerId);
    }

    //* Match Game ======================================
    //큐 등록
    pushQueue(playerId: string, gameMode: number, userId: number) {
        this.updatePlayer(playerId, userId, gameMode, GameType.MATCH);
        if (this.matchQueue[gameMode]) {
            const playerQ = this.matchQueue[gameMode];
            this.matchQueue[gameMode] = undefined;
            this.makeGameRoom(playerQ, playerId, GameType.MATCH, gameMode);
        } else this.matchQueue[gameMode] = playerId;
    }

    popQueue(playerId: string): void {
        if (
            this.matchQueue[this.playerList.get(playerId).gameMode] === playerId
        ) {
            this.matchQueue[this.playerList.get(playerId).gameMode] = undefined;
            this.resetPlayer(this.playerList.get(playerId));
        } else throw new BadRequestException('player was not in Queue');
    }

    //* Friend Game ======================================
    inviteGame(
        playerId: string,
        gameMode: number,
        userId: number,
        friendId: number,
    ) {
        this.updatePlayer(
            playerId,
            userId,
            gameMode,
            GameType.FRIEND,
            friendId,
        );
        this.friendGameList.set(userId, playerId);
        console.log('inviteGame:', userId, 'waiting for', friendId);
    }

    agreeInvite(playerId: string, userId: number, friendId: number) {
        //queue에 초대한 친구 있는지 확인
        const hostId = this.friendGameList.get(friendId);
        if (hostId && this.playerList.get(hostId).friendId === userId) {
            //friendlist에서 해당 큐 제외
            this.friendGameList.delete(friendId);
            //player update
            const gameMode = this.playerList.get(hostId).gameMode;
            this.updatePlayer(
                playerId,
                userId,
                gameMode,
                GameType.FRIEND,
                friendId,
            );
            console.log('invite connected:', userId, '&', friendId);
            //방파줌. 즐겜! << ㅋㅋ
            this.makeGameRoom(hostId, playerId, GameType.FRIEND, gameMode);
        } else {
            // 초대한 친구 없거나 초대한 사람이 내가 아니야!! => kickout 발동!
            this.resetPlayer(this.playerList.get(playerId));
            this.playerList.get(playerId).socket.emit('kickout');
        }
    }

    denyInvite(playerId: string, userId: number, friendId: number) {
        //queue에 초대한 친구 있는지 확인
        const hostId = this.friendGameList.get(friendId);
        if (hostId && this.playerList.get(hostId).friendId === userId) {
            this.friendGameList.delete(friendId);
            this.playerList.get(hostId).socket.emit('denyInvite');
        } else throw new BadRequestException('no invitation to deny');
    }

    //* Game Room ======================================
    getReady(playerId: string): void {
        const roomId = this.playerList.get(playerId).roomId;
        const side = this.playerList.get(playerId).side;
        const rivalSide = (side + 1) % 2;
        this.gameRoomList.get(roomId).ready[side] = true;
        if (this.gameRoomList.get(roomId).ready[rivalSide]) {
            //둘 다 게임 준비 완료!
            this.updateGame(roomId);
            const [leftPlayer, rightPlayer] =
                this.gameRoomList.get(roomId).socket;
            const gameInfo = this.getBallStartDir(roomId);
            gameInfo['isFirst'] = true;
            gameInfo['leftScore'] = 0;
            gameInfo['rightScore'] = 0;
            gameInfo['ballSpeed'] =
                BALL_SPEED[this.gameRoomList.get(roomId).gameMode];
            gameInfo['side'] = PlayerSide.LEFT;
            leftPlayer.emit('startGame', gameInfo);
            gameInfo['side'] = PlayerSide.RIGHT;
            rightPlayer.emit('startGame', gameInfo);
            console.log('>>>>>> emit startGame done');
        }
    }

    //* In Game ======================================
    movePaddle(playerId: string, gameInfo: JSON) {
        const roomId = this.playerList.get(playerId).roomId;
        const side = this.playerList.get(playerId).side;
        const rival = this.gameRoomList.get(roomId).socket[(side + 1) % 2];
        // if (
        //     gameInfo['paddlePosX'] !== PADDLE_POS_X[side] ||
        //     gameInfo['paddlePosY'] !== PADDLE_POS_Y ||
        //     gameInfo['paddlePosZ'] < PADDLE_POS_Z_MIN ||
        //     gameInfo['paddlePosZ'] > PADDLE_POS_Z_MAX
        // ) {
        //     this.finishGame(roomId, (side + 1) % 2, GameEndStatus.CHEATING);
        //     return;
        // }
        rival.emit('movePaddle', {
            paddlePosX: PADDLE_POS_X[side],
            paddlePosY: PADDLE_POS_Y,
            paddlePosZ: gameInfo['paddlePosZ'],
        });
    }

    sendEmoji(playerId: string, emoji: string) {
        if (+emoji < Emoji.HI || Emoji.BADWORDS < +emoji)
            throw new BadRequestException('wrong emoji sent');
        const player = this.playerList.get(playerId);
        const side = player.side;
        const rivalSocket = this.gameRoomList.get(player.roomId).socket[
            (side + 1) % 2
        ];
        rivalSocket.emit('sendEmoji', { type: emoji });
    }

    async validCheck(playerId: string, gameInfo: JSON) {
        const player = this.playerList.get(playerId);
        //TESTCODE: to be deleted
        if (BALL_SPEED[player.gameMode] === gameInfo['ballSpeed'])
            console.log('>>>>>>>> json number okay');
        else console.log('>>>>>>>> JSON number NOT okay: modify immediately');
        //end of TESTCODE
        if (
            //ball
            BALL_SPEED[player.gameMode] !== gameInfo['ballSpeed'] ||
            BALL_POS_X_MIN >= gameInfo['ballPosX'] ||
            BALL_POS_X_MAX <= gameInfo['ballPosX'] ||
            BALL_POS_Y_MIN >= gameInfo['ballPosY'] ||
            BALL_POS_Y_MAX <= gameInfo['ballPosY'] ||
            BALL_POS_Z_MIN >= gameInfo['ballPosZ'] ||
            BALL_POS_Z_MAX <= gameInfo['ballPosZ'] ||
            BALL_SCALE_X !== gameInfo['ballScaleX'] ||
            BALL_SCALE_Y !== gameInfo['ballScaleY'] ||
            BALL_SCALE_Z !== gameInfo['ballScaleZ'] ||
            //left
            PADDLE_POS_X[PlayerSide.LEFT] !== gameInfo['leftPosX'] ||
            PADDLE_POS_Y !== gameInfo['leftPosY'] ||
            PADDLE_POS_Z_MIN >= gameInfo['leftPosZ'] ||
            PADDLE_POS_Z_MAX <= gameInfo['leftPosZ'] ||
            PADDLE_ROTATE_X !== gameInfo['leftRotateX'] ||
            PADDLE_ROTATE_Y !== gameInfo['leftRotateY'] ||
            PADDLE_ROTATE_Z !== gameInfo['leftRotateZ'] ||
            PADDLE_SCALE_X !== gameInfo['leftScaleX'] ||
            PADDLE_SCALE_Y !== gameInfo['leftScaleY'] ||
            PADDLE_SCALE_Z !== gameInfo['leftScaleZ'] ||
            PADDLE_SPEED !== gameInfo['leftSpeed'] ||
            //right
            PADDLE_POS_X[PlayerSide.RIGHT] !== gameInfo['rightPosX'] ||
            PADDLE_POS_Y !== gameInfo['rightPosY'] ||
            PADDLE_POS_Z_MIN >= gameInfo['rightPosZ'] ||
            PADDLE_POS_Z_MAX <= gameInfo['rightPosZ'] ||
            PADDLE_ROTATE_X !== gameInfo['rightRotateX'] ||
            PADDLE_ROTATE_Y !== gameInfo['rightRotateY'] ||
            PADDLE_ROTATE_Z !== gameInfo['rightRotateZ'] ||
            PADDLE_SCALE_X !== gameInfo['rightScaleX'] ||
            PADDLE_SCALE_Y !== gameInfo['rightScaleY'] ||
            PADDLE_SCALE_Z !== gameInfo['rightScaleZ'] ||
            PADDLE_SPEED !== gameInfo['rightSpeed']
        ) {
            console.log('validCheck: cheating detacted');
            const roomId = player.roomId;
            const rivalSide = (player.side + 1) % 2;
            await this.finishGame(roomId, rivalSide, GameEndStatus.CHEATING);
        }
    }

    async ballHit(leftId: string, gameInfo: JSON) {
        const roomId = this.playerList.get(leftId).roomId;
        const rivalSocket =
            this.gameRoomList.get(roomId).socket[PlayerSide.RIGHT];
        //validCheck
        if (this.isBallOkay(roomId, gameInfo) === false) {
            console.log('ballHit: cheating detacted');
            await this.finishGame(
                roomId,
                PlayerSide.RIGHT,
                GameEndStatus.CHEATING,
            );
            return;
        }
        //score check
        if (
            gameInfo['ballPosX'] <= BALL_POS_X_MIN + PADDING + 0.75 ||
            gameInfo['ballPosX'] >= BALL_POS_X_MAX - PADDING - 0.75
        ) {
            let scoreSide = PlayerSide.RIGHT;
            if (gameInfo['ballPosX'] >= BALL_POS_X_MAX - PADDING - 0.75)
                scoreSide = PlayerSide.LEFT;
            this.gameRoomList.get(roomId).score[scoreSide] += 1;
            if (this.gameRoomList.get(roomId).score[scoreSide] === MAXSCORE) {
                //game finish (max score reached)
                await this.finishGame(roomId, scoreSide, GameEndStatus.NORMAL);
            } else this.continueGame(roomId);
        } else {
            //받아침! => ballInfo update & rival에게 전달
            this.updateBall(
                roomId,
                gameInfo['ballDirX'],
                gameInfo['ballDirZ'],
                gameInfo['ballPosX'],
                gameInfo['ballPosZ'],
            );
            rivalSocket.emit('ballHit', gameInfo);
        }
    }

    //* other functions ======================================

    //득점 후 계속 진행
    continueGame(roomId: number) {
        const [player1, player2] = this.gameRoomList.get(roomId).socket;
        const gameInfo = this.getBallStartDir(roomId);
        gameInfo['isFirst'] = false;
        gameInfo['side'] = PlayerSide.NONE; //아무값
        gameInfo['leftScore'] =
            this.gameRoomList.get(roomId).score[PlayerSide.LEFT];
        gameInfo['rightScore'] =
            this.gameRoomList.get(roomId).score[PlayerSide.RIGHT];
        gameInfo['ballSpeed'] =
            BALL_SPEED[this.gameRoomList.get(roomId).gameMode];
        player1.emit('startGame', gameInfo);
        player2.emit('startGame', gameInfo);
    }

    //게임 종료 (정상 + 비정상)
    async finishGame(
        roomId: number,
        winnerSide: number,
        endGameStatus: number,
    ) {
        //gameRoom 업데이트
        this.updateGameRoom(roomId, winnerSide, endGameStatus);
        // console.log('gameRoom: ', this.gameRoomList.get(roomId));
        //플레이어들에게 결과 전달
        const [player1Socket, player2Socket] =
            this.gameRoomList.get(roomId).socket;
        const gameResult = {
            winner: winnerSide,
            leftScore: this.gameRoomList.get(roomId).score[PlayerSide.LEFT],
            rightScore: this.gameRoomList.get(roomId).score[PlayerSide.RIGHT],
            reason: endGameStatus,
        };
        player1Socket.emit('gameOver', gameResult);
        player2Socket.emit('gameOver', gameResult);
        console.log('sent gameOver:', gameResult);
        //DB에 저장
        await this.createGameData(roomId);
        //gameRoom 처리
        if (
            this.gameRoomList.get(roomId).gameType === GameType.FRIEND &&
            endGameStatus === GameEndStatus.NORMAL
        ) {
            //친선경기 => reset gameRoom for restart
            this.resetGameRoom(roomId);
        } else {
            //그 외 => player reset && delete room
            this.resetPlayer(this.playerList.get(player1Socket.id));
            this.resetPlayer(this.playerList.get(player2Socket.id));
            if (this.gameRoomList.get(roomId)) this.gameRoomList.delete(roomId);
        }
    }

    resetGameRoom(roomId: number) {
        this.gameRoomList.get(roomId).ready = [false, false];
        this.gameRoomList.get(roomId).gameStatus = GameStatus.WAIT;
        this.gameRoomList.get(roomId).score = [0, 0];
        this.gameRoomList.get(roomId).startTime = undefined;
        this.gameRoomList.get(roomId).endTime = undefined;
        this.gameRoomList.get(roomId).winner = undefined;
        this.gameRoomList.get(roomId).loser = undefined;
        this.gameRoomList.get(roomId).endGameStatus = undefined;
    }
    //게임 종료시 gameRoom 업데이트
    updateGameRoom(roomId: number, side: number, endGameStatus: number) {
        this.gameRoomList.get(roomId).endTime =
            DateTime.now().setZone(TIMEZONE);
        this.gameRoomList.get(roomId).winner = side;
        this.gameRoomList.get(roomId).loser = (side + 1) % 2;
        this.gameRoomList.get(roomId).endGameStatus = endGameStatus;
        if (
            endGameStatus === GameEndStatus.CHEATING ||
            endGameStatus === GameEndStatus.DISCONNECT
        ) {
            this.gameRoomList.get(roomId).score[side] = MAXSCORE;
            this.gameRoomList.get(roomId).score[(side + 1) % 2] = 0;
        }
    }
    //DB에 게임 결과 저장
    async createGameData(roomId: number) {
        try {
            const room = this.gameRoomList.get(roomId);
            const [winnerUserId, loserUserId] = [
                this.playerList.get(room.socket[room.winner].id).userId,
                this.playerList.get(room.socket[room.loser].id).userId,
            ];
            const newGameData = this.gameRepository.create({
                winnerId: winnerUserId,
                winnerScore: room.score[room.winner],
                winnerSide: room.winner,
                loserId: loserUserId,
                loserScore: room.score[room.loser],
                loserSide: room.loser,
                gameType: room.gameType,
                gameMode: room.gameMode,
                startTime: room.startTime,
                endTime: room.endTime,
                endGameStatus: room.endGameStatus,
            } as Game);
            //! un-comment below
            //await this.gameRepository.save(newGameData);
        } catch (error) {
            //TESTCODE
            console.log('Error: game =>', error);
            throw new InternalServerErrorException(
                'error while save game data',
            );
        }
    }

    //gameRoom 만들기
    makeGameRoom(
        player1Id: string,
        player2Id: string,
        gameType: number,
        gameMode: number,
    ) {
        //waitRoom 만들기
        const gameRoom: GameRoom = {
            id: this.gameRoomIdx,
            gameType: gameType,
            gameMode: gameMode,
            gameStatus: GameStatus.WAIT,
            ready: [false, false],
        };
        //TESTCODE
        console.log('makeGameRoom: ', player1Id, player2Id);
        //방입장
        this.enterGameRoom(gameRoom, player1Id, player2Id);
        this.gameRoomList.set(this.gameRoomIdx++, gameRoom);
    }

    // players enter the gameRoom
    enterGameRoom(gameRoom: GameRoom, player1Id: string, player2Id: string) {
        let left: string, right: string;
        if (Math.floor(Math.random() * 2)) {
            left = player1Id;
            right = player2Id;
        } else {
            left = player2Id;
            right = player1Id;
        }
        gameRoom.socket = [
            this.playerList.get(left).socket,
            this.playerList.get(right).socket,
        ];

        //Player 업데이트 && emit('handShake')
        console.log('enterGameRoom:', gameRoom.id);
        this.handShake(left, PlayerSide.LEFT, gameRoom.id);
        this.handShake(right, PlayerSide.RIGHT, gameRoom.id);
    }
    //gameRoom 진입 시 Player 정보 업데이트
    handShake(playerId: string, side: number, roomId: number): void {
        this.playerList.get(playerId).roomId = roomId;
        this.playerList.get(playerId).side = side;
        this.playerList.get(playerId).socket.emit('handShake');
    }

    //game 시작 시 => gameRoom 업데이트
    updateGame(gameRoomId: number) {
        this.gameRoomList.get(gameRoomId).gameStatus = GameStatus.GAME;
        this.gameRoomList.get(gameRoomId).startTime =
            DateTime.now().setZone(TIMEZONE);
        this.gameRoomList.get(gameRoomId).score = [0, 0];
        //TESTCODE: startTime
        // console.log('startTime:', this.gameRoomList.get(gameRoomId).startTime);
    }

    updatePlayer(
        playerId: string,
        userId: number,
        gameMode: number,
        gameType: number,
        friendId?: number | undefined,
    ) {
        const player: Player = this.playerList.get(playerId);
        if (player.roomId)
            throw new BadRequestException('player already in gameRoom');
        player.userId = userId;
        if (friendId) player.friendId = friendId;
        player.gameType = gameType;
        player.gameMode = gameMode;
        if (gameMode === GameMode.NONE)
            throw new BadRequestException('gameMode invalid');
    }

    //game 각 라운드 시작 시 공 방향 세팅
    getBallStartDir(roomId: number): any {
        const dirX = (Math.random() * (MAX - MIN) + MIN) * -2 + 1;
        const dirZ =
            ((Math.random() * (MAX - MIN) + MIN) * -2 + 1) *
            (Math.random() * (MAXF - MINF) + MINF);
        this.updateBall(roomId, dirX, dirZ);
        return {
            ballDirX: dirX,
            ballDirY: 0,
            ballDirZ: dirZ,
        };
    }
    //ball 정보 업데이트
    updateBall(
        roomId: number,
        dirX: number,
        dirZ: number,
        posX?: number,
        posZ?: number,
    ) {
        this.gameRoomList.get(roomId).dirX = dirX;
        this.gameRoomList.get(roomId).dirZ = dirZ;
        this.gameRoomList.get(roomId).posX = posX !== undefined ? posX : 0;
        this.gameRoomList.get(roomId).posZ = posZ !== undefined ? posZ : 0;
    }
    //ballHit valid check
    isBallOkay(roomId: number, gameInfo: JSON): boolean {
        const diffPosXZ =
            (gameInfo['ballPosX'] - this.gameRoomList.get(roomId).posX) /
            (gameInfo['ballPosZ'] - this.gameRoomList.get(roomId).posZ);

        const dirXZ =
            this.gameRoomList.get(roomId).dirX /
            this.gameRoomList.get(roomId).dirZ;

        if (
            diffPosXZ - dirXZ < -2 * PADDING ||
            2 * PADDING < diffPosXZ - dirXZ ||
            BALL_POS_X_MIN >= gameInfo['ballPosX'] ||
            BALL_POS_X_MAX <= gameInfo['ballPosX'] ||
            BALL_POS_Y_MIN >= gameInfo['ballPosY'] ||
            BALL_POS_Y_MAX <= gameInfo['ballPosY'] ||
            BALL_POS_Z_MIN >= gameInfo['ballPosZ'] ||
            BALL_POS_Z_MAX <= gameInfo['ballPosZ']
        )
            return false;
        return true;
    }

    //게임 중단/종료 시 플레이어 리셋
    resetPlayer(player: Player) {
        player.gameType = undefined;
        player.gameMode = undefined;
        player.side = undefined;
        player.roomId = undefined;
        player.friendId = undefined;
    }
}
