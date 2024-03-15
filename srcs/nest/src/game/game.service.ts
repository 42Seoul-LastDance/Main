/* eslint-disable prettier/prettier */
import {
    BadRequestException,
    Inject,
    Injectable,
    InternalServerErrorException,
    Logger,
    forwardRef,
} from '@nestjs/common';
import { Socket } from 'socket.io';
import { GameType, GameMode, PlayerSide, GameStatus, GameEndStatus, Emoji } from './game.enum';
import {
    MAXSCORE,
    TIMEZONE,
    BALL_SPEED,
    BALL_POS_X_MIN,
    BALL_POS_X_MAX,
    BALL_POS_Y,
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
    EPSILON,
    SCORE_PADDING,
    BALL_PADDING,
} from './game.constants';
import { GameRoom, Player, GameData, FriendGameData } from './game.interface';
import { Game } from './game.entity';
import { InjectRepository } from '@nestjs/typeorm';
import { GameRepository } from './game.repository';
import { DateTime } from 'luxon';
import { UserService } from 'src/user/user.service';
import { SocketUsersService } from '../socket/socketUsersService/socketUsers.service';
import { User } from 'src/user/user.entity';

@Injectable()
export class GameService {
    private logger = new Logger(GameService.name);
    constructor(
        @InjectRepository(Game)
        private gameRepository: GameRepository,
        @Inject(forwardRef(() => UserService))
        private userService: UserService,
        private readonly socketUsersService: SocketUsersService,
    ) {}

    //Queue
    private matchQueue: [string, string] = [undefined, undefined]; //socket.id
    private friendGameList: Map<string, string> = new Map<string, string>(); //username(host), socket.id
    //Room
    private gameRoomIdx: number = 0;
    private gameRoomList: Map<number, GameRoom> = new Map<number, GameRoom>(); //room.id

    //PlayerList에 등록
    async createPlayer(playerSocket: Socket, userId: string) {
        try {
            const prevSocketId: string = this.socketUsersService.getGameUserList().get(+userId);
            if (prevSocketId) {
                const prevSocket: Socket = this.socketUsersService.getPlayerBySocketId(prevSocketId).socket;
                prevSocket.disconnect(false);
            }
            const user: User = await this.userService.findUserById(+userId);
            if (user === undefined) throw new BadRequestException('no such userId');
            const player: Player = {
                socket: playerSocket,
                userId: +userId,
                userName: user.userName,
            };
            this.socketUsersService.addPlayerBySocketId(playerSocket.id, player);
            await this.socketUsersService.addGameUserList(+userId, playerSocket.id);
        } catch (error) {
            console.log('error >> gameService >> createPlayer');
            throw new InternalServerErrorException('[ERR] gameService >> createPlayer');
        }
    }

    //disconnect 시 처리
    async handleDisconnect(playerId: string) {
        try {
            const player = this.socketUsersService.getPlayerBySocketId(playerId);
            if (player === undefined || player.userId === undefined) return;
            if (player.roomId === undefined) {
                // in Queue => queue에서 제거
                // console.log('GAME>>>> discon >>> remove from queue');
                if (player.gameType === GameType.MATCH) {
                    if (this.matchQueue[player.gameMode] === player.socket.id)
                        this.matchQueue[player.gameMode] = undefined;
                } else this.quitInvite(playerId);
            } else {
                //in room (waiting or game)
                const gameRoom = this.gameRoomList.get(player.roomId);
                if (gameRoom.gameStatus === GameStatus.WAIT) {
                    // console.log('GAME>>>> discon >>> kickout');
                    //in waiting Room => 상대방에게 대기방 나가기 이벤트 발생!
                    const rival: Player = this.socketUsersService.getPlayerBySocketId(
                        gameRoom.socket[(player.side + 1) % 2].id,
                    );
                    this.resetPlayer(rival);
                    rival.socket.emit('kickout');
                } else if (gameRoom.gameStatus === GameStatus.GAME) {
                    // console.log('GAME>>>> discon >>> finishGame');
                    //게임중 => 게임 강제종료
                    await this.finishGame(gameRoom.id, (player.side + 1) % 2, GameEndStatus.DISCONNECT);
                }
                //gameroom 없애기
                this.gameRoomList.delete(player.roomId);
            }
            await this.socketUsersService.deleteGameUserList(player.userId, playerId);
        } catch (error) {
            console.log('error >> gameService >> handleDisconnect');
            throw new InternalServerErrorException('[ERR] gameService >> handleDisconnect');
        }
    }
    //Player 삭제 (handleDisconnect용)
    deletePlayer(playerId: string) {
        this.resetPlayer(this.socketUsersService.getPlayerBySocketId(playerId));
        // console.log('entering >> gameService >> deletePlayer');
        this.socketUsersService.deletePlayer(playerId);
    }

    //* Match Game ======================================
    //큐 등록
    async pushQueue(playerId: string, gameMode: number) {
        try {
            this.updatePlayer(playerId, gameMode, GameType.MATCH);
            if (this.matchQueue[gameMode] !== undefined) {
                const playerQ = this.matchQueue[gameMode];
                this.matchQueue[gameMode] = undefined;
                await this.makeGameRoom(playerQ, playerId, GameType.MATCH, gameMode);
            } else this.matchQueue[gameMode] = playerId;
        } catch (error) {
            console.log('error >> gameService >> pushQueue');
            throw new InternalServerErrorException('[ERR] gameService >> pushQueue');
        }
    }

    popQueue(playerId: string): void {
        if (this.matchQueue[this.socketUsersService.getPlayerBySocketId(playerId).gameMode] === playerId) {
            this.matchQueue[this.socketUsersService.getPlayerBySocketId(playerId).gameMode] = undefined;
            this.resetPlayer(this.socketUsersService.getPlayerBySocketId(playerId));
        } //else throw new BadRequestException('player was not in Queue');
    }

    //* Friend Game ======================================
    async inviteGame(playerId: string, gameMode: number, friendName: string) {
        try {
            const hostPlayer = this.socketUsersService.getPlayerBySocketId(playerId);
            const friend: User = await this.userService.getUserByUserName(friendName);
            if (friend === undefined) throw new BadRequestException('no such friendName');
            if (hostPlayer.userId === friend.id) {
                hostPlayer.socket.emit('kickout');
                return;
            }
            this.updatePlayer(playerId, gameMode, GameType.FRIEND, friend.id);
            this.friendGameList.set(hostPlayer.userName, playerId);
            // console.log('inviteGame:', hostName, 'waiting for', friendName);
        } catch (error) {
            console.log('error >> gameService >> inviteGame');
            throw new InternalServerErrorException('[ERR] gameService >> inviteGame');
        }
    }

    async agreeInvite(playerId: string, friendName: string) {
        //queue에 초대한 친구 있는지 확인
        const hostId = this.friendGameList.get(friendName);
        try {
            if (
                hostId &&
                this.socketUsersService.getPlayerBySocketId(hostId).friendId ===
                    this.socketUsersService.getPlayerBySocketId(playerId).userId
            ) {
                //friendlist에서 해당 큐 제외
                this.friendGameList.delete(friendName);
                //player update
                const gameMode = this.socketUsersService.getPlayerBySocketId(hostId).gameMode;
                this.updatePlayer(
                    playerId,
                    gameMode,
                    GameType.FRIEND,
                    this.socketUsersService.getPlayerBySocketId(hostId).userId,
                );
                //방파줌. 즐겜! << ㅋㅋ
                await this.makeGameRoom(hostId, playerId, GameType.FRIEND, gameMode);
            } else {
                // 초대한 친구 없거나 초대한 사람이 내가 아니야!! => kickout 발동!
                this.resetPlayer(this.socketUsersService.getPlayerBySocketId(playerId));
                this.socketUsersService.getPlayerBySocketId(playerId).socket.emit('kickout');
            }
        } catch (error) {
            console.log('error >> gameService >> agreeInvite');
            throw new InternalServerErrorException('[ERR] gameService >> agreeInvite');
        }
    }

    quitInvite(playerId: string) {
        const hostPlayer: Player = this.socketUsersService.getPlayerBySocketId(playerId);
        if (hostPlayer && this.friendGameList.get(hostPlayer.userName)) {
            const guestSocket: Socket = this.socketUsersService.getDMSocketById(hostPlayer.friendId);
            if (guestSocket) {
                guestSocket.emit('updateInvitation');
                guestSocket.emit('invitationSize', this.getInvitationSize(hostPlayer.friendId));
            }
            this.friendGameList.delete(hostPlayer.userName);
        }
    }

    getInvitationSize(userId: number) {
        return this.socketUsersService.getInviteListByUserId(userId).size;
    }

    denyInvite(playerId: string, friendName: string) {
        //queue에 초대한 친구 있는지 확인
        const hostId = this.friendGameList.get(friendName);
        if (
            hostId &&
            this.socketUsersService.getPlayerBySocketId(hostId).friendId ===
                this.socketUsersService.getPlayerBySocketId(playerId).userId
        ) {
            this.friendGameList.delete(friendName);
            this.socketUsersService.getPlayerBySocketId(hostId).socket.emit('denyInvite');
        }
    }

    //* Game Room ======================================
    getReady(playerId: string): void {
        const roomId = this.socketUsersService.getPlayerBySocketId(playerId).roomId;
        if (roomId === undefined) return;
        const side = this.socketUsersService.getPlayerBySocketId(playerId).side;
        const rivalSide = (side + 1) % 2;
        this.gameRoomList.get(roomId).ready[side] = true;
        if (this.gameRoomList.get(roomId).ready[rivalSide]) {
            //둘 다 게임 준비 완료!
            this.updateGame(roomId);
            const [leftPlayer, rightPlayer] = this.gameRoomList.get(roomId).socket;
            const gameInfo = this.getBallStartDir(roomId);
            gameInfo['isFirst'] = true;
            gameInfo['leftName'] = this.gameRoomList.get(roomId).name[PlayerSide.LEFT];
            gameInfo['rightName'] = this.gameRoomList.get(roomId).name[PlayerSide.RIGHT];
            gameInfo['leftScore'] = 0;
            gameInfo['rightScore'] = 0;
            gameInfo['ballSpeed'] = BALL_SPEED[this.gameRoomList.get(roomId).gameMode];
            gameInfo['side'] = PlayerSide.LEFT;
            leftPlayer.emit('startGame', gameInfo);
            gameInfo['side'] = PlayerSide.RIGHT;
            rightPlayer.emit('startGame', gameInfo);
            // console.log('>>>>>> emit startGame done');
        }
    }

    //* In Game ======================================
    movePaddle(playerId: string, gameInfo: JSON) {
        const roomId = this.socketUsersService.getPlayerBySocketId(playerId).roomId;
        if (roomId === undefined) return;
        const side = this.socketUsersService.getPlayerBySocketId(playerId).side;
        const rival = this.gameRoomList.get(roomId).socket[(side + 1) % 2];
        if (
            !this.equals(gameInfo['paddlePosX'], PADDLE_POS_X[side]) ||
            !this.equals(gameInfo['paddlePosY'], PADDLE_POS_Y) ||
            gameInfo['paddlePosZ'] < PADDLE_POS_Z_MIN ||
            gameInfo['paddlePosZ'] > PADDLE_POS_Z_MAX
        ) {
            await this.finishGame(roomId, (side + 1) % 2, GameEndStatus.CHEATING);
            return;
        }
        rival.emit('movePaddle', {
            paddlePosX: gameInfo['paddlePosX'],
            paddlePosY: gameInfo['paddlePosY'],
            paddlePosZ: gameInfo['paddlePosZ'],
        });
    }

    sendEmoji(playerId: string, emoji: string) {
        if (+emoji < Emoji.MIN || Emoji.MAX < +emoji) return;
        const player = this.socketUsersService.getPlayerBySocketId(playerId);
        if (player.roomId === undefined) return;
        const side = player.side;
        const rivalSocket = this.gameRoomList.get(player.roomId).socket[(side + 1) % 2];
        rivalSocket.emit('sendEmoji', { type: emoji });
    }

    async validCheck(playerId: string, gameInfo: JSON) {
        const player = this.socketUsersService.getPlayerBySocketId(playerId);
        if (player.roomId === undefined) return;
        //end of TESTCODE
        if (
            //ball
            BALL_SPEED[player.gameMode] !== gameInfo['ballSpeed'] ||
            // BALL_POS_X_MIN - BALL_PADDING > gameInfo['ballPosX'] ||
            // BALL_POS_X_MAX + BALL_PADDING < gameInfo['ballPosX'] ||
            !this.equals(gameInfo['ballPosY'], BALL_POS_Y) ||
            // BALL_POS_Z_MIN - BALL_PADDING > gameInfo['ballPosZ'] ||
            // BALL_POS_Z_MAX + BALL_PADDING < gameInfo['ballPosZ'] ||
            BALL_SCALE_X !== gameInfo['ballScaleX'] ||
            BALL_SCALE_Y !== gameInfo['ballScaleY'] ||
            BALL_SCALE_Z !== gameInfo['ballScaleZ'] ||
            //left
            !this.equals(gameInfo['leftPosX'], PADDLE_POS_X[PlayerSide.LEFT]) ||
            !this.equals(gameInfo['leftPosY'], PADDLE_POS_Y) ||
            PADDLE_POS_Z_MIN > gameInfo['leftPosZ'] ||
            PADDLE_POS_Z_MAX < gameInfo['leftPosZ'] ||
            PADDLE_ROTATE_X !== gameInfo['leftRotateX'] ||
            PADDLE_ROTATE_Y !== gameInfo['leftRotateY'] ||
            PADDLE_ROTATE_Z !== gameInfo['leftRotateZ'] ||
            !this.equals(gameInfo['leftScaleX'], PADDLE_SCALE_X) ||
            !this.equals(gameInfo['leftScaleY'], PADDLE_SCALE_Y) ||
            PADDLE_SCALE_Z !== gameInfo['leftScaleZ'] ||
            PADDLE_SPEED !== gameInfo['leftSpeed'] ||
            //right
            !this.equals(gameInfo['rightPosX'], PADDLE_POS_X[PlayerSide.RIGHT]) ||
            !this.equals(gameInfo['rightPosY'], PADDLE_POS_Y) ||
            PADDLE_POS_Z_MIN > gameInfo['rightPosZ'] ||
            PADDLE_POS_Z_MAX < gameInfo['rightPosZ'] ||
            PADDLE_ROTATE_X !== gameInfo['rightRotateX'] ||
            PADDLE_ROTATE_Y !== gameInfo['rightRotateY'] ||
            PADDLE_ROTATE_Z !== gameInfo['rightRotateZ'] ||
            !this.equals(gameInfo['rightScaleX'], PADDLE_SCALE_X) ||
            !this.equals(gameInfo['rightScaleY'], PADDLE_SCALE_Y) ||
            PADDLE_SCALE_Z !== gameInfo['rightScaleZ'] ||
            PADDLE_SPEED !== gameInfo['rightSpeed']
        ) {
            // console.log('validCheck: cheating detacted');
            const roomId = player.roomId;
            const rivalSide = (player.side + 1) % 2;
            try {
                await this.finishGame(roomId, rivalSide, GameEndStatus.CHEATING);
            } catch (error) {
                console.log('error >> gameService >> validCheck');
                throw new InternalServerErrorException('[ERR] gameService >> validCheck');
            }
        }
    }

    async ballHit(leftId: string, gameInfo: JSON) {
        const roomId = this.socketUsersService.getPlayerBySocketId(leftId).roomId;
        if (roomId === undefined) return;
        const rivalSocket = this.gameRoomList.get(roomId).socket[PlayerSide.RIGHT];
        //score check
        try {
            if (gameInfo['score'] === true) {
                let scoreSide = PlayerSide.RIGHT;
                if (gameInfo['ballPosX'] >= BALL_POS_X_MAX - SCORE_PADDING) scoreSide = PlayerSide.LEFT;
                this.gameRoomList.get(roomId).score[scoreSide] += 1;
                if (this.gameRoomList.get(roomId).score[scoreSide] === MAXSCORE) {
                    //game finish (max score reached)
                    await this.finishGame(roomId, scoreSide, GameEndStatus.NORMAL);
                } else this.continueGame(roomId);
            } else {
                this.updateBall(
                    roomId,
                    gameInfo['ballDirX'],
                    gameInfo['ballDirZ'],
                    gameInfo['ballPosX'],
                    gameInfo['ballPosZ'],
                );
                rivalSocket.emit('ballHit', gameInfo);
            }
        } catch (error) {
            console.log('error >> gameService >> ballHit');
            throw new InternalServerErrorException('[ERR] gameService >> ballHit');
        }
    }

    async outGame(playerId: string) {
        try {
            const roomId = this.socketUsersService.getPlayerBySocketId(playerId).roomId;
            if (roomId === undefined) return;
            if (this.gameRoomList.get(roomId).gameStatus === GameStatus.WAIT) return;
            const rivalSide = (this.socketUsersService.getPlayerBySocketId(playerId).side + 1) % 2;
            await this.finishGame(roomId, rivalSide, GameEndStatus.OUTGAME);
        } catch (error) {
            console.log('error >> gameService >> outGame');
            throw new InternalServerErrorException('[ERR] gameService >> outGame');
        }
    }

    //* other functions ======================================

    //득점 후 계속 진행
    continueGame(roomId: number) {
        const [player1, player2] = this.gameRoomList.get(roomId).socket;
        const gameInfo = this.getBallStartDir(roomId);
        gameInfo['isFirst'] = false;
        gameInfo['side'] = PlayerSide.NONE; //아무값
        gameInfo['leftName'] = this.gameRoomList.get(roomId).name[PlayerSide.LEFT];
        gameInfo['rightName'] = this.gameRoomList.get(roomId).name[PlayerSide.RIGHT];
        gameInfo['leftScore'] = this.gameRoomList.get(roomId).score[PlayerSide.LEFT];
        gameInfo['rightScore'] = this.gameRoomList.get(roomId).score[PlayerSide.RIGHT];
        gameInfo['ballSpeed'] = BALL_SPEED[this.gameRoomList.get(roomId).gameMode];
        player1.emit('startGame', gameInfo);
        player2.emit('startGame', gameInfo);
    }

    //게임 종료 (정상 + 비정상)
    async finishGame(roomId: number, winnerSide: number, endGameStatus: number) {
        if (this.gameRoomList.get(roomId) === undefined) return;
        //gameRoom 업데이트
        this.updateGameRoom(roomId, winnerSide, endGameStatus);
        //플레이어들에게 결과 전달
        const [player1Socket, player2Socket] = this.gameRoomList.get(roomId).socket;
        const gameResult = {
            winner: winnerSide,
            leftScore: this.gameRoomList.get(roomId).score[PlayerSide.LEFT],
            rightScore: this.gameRoomList.get(roomId).score[PlayerSide.RIGHT],
            reason: endGameStatus,
        };
        player1Socket.emit('gameOver', gameResult);
        player2Socket.emit('gameOver', gameResult);
        // console.log('sent gameOver:', gameResult);
        try {
            //DB에 저장
            await this.createGameData(roomId);
            //gameRoom 처리
            if (
                this.gameRoomList.get(roomId).gameType === GameType.FRIEND &&
                endGameStatus !== GameEndStatus.DISCONNECT
            ) {
                //친선경기 => reset gameRoom for restart
                this.resetGameRoom(roomId);
            } else {
                //그 외 => player reset && delete room
                if (endGameStatus === GameEndStatus.DISCONNECT) {
                    this.resetPlayer(
                        this.socketUsersService.getPlayerBySocketId(
                            this.gameRoomList.get(roomId).socket[winnerSide].id,
                        ),
                    );
                    this.resetPlayer(
                        this.socketUsersService.getPlayerBySocketId(
                            this.gameRoomList.get(roomId).socket[(winnerSide + 1) % 2].id,
                        ),
                    );
                } else {
                    this.resetPlayer(this.socketUsersService.getPlayerBySocketId(player1Socket.id));
                    this.resetPlayer(this.socketUsersService.getPlayerBySocketId(player2Socket.id));
                }
                if (this.gameRoomList.get(roomId)) this.gameRoomList.delete(roomId);
            }
        } catch (error) {
            console.log('error >> gameService >> finishGame');
            throw new InternalServerErrorException('[ERR] gameService >> finishGame');
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
        this.gameRoomList.get(roomId).endTime = DateTime.now().setZone(TIMEZONE);
        this.gameRoomList.get(roomId).winner = side;
        this.gameRoomList.get(roomId).loser = (side + 1) % 2;
        this.gameRoomList.get(roomId).endGameStatus = endGameStatus;
        if (endGameStatus !== GameEndStatus.NORMAL) {
            this.gameRoomList.get(roomId).score[side] = MAXSCORE;
            this.gameRoomList.get(roomId).score[(side + 1) % 2] = 0;
        }
    }
    //DB에 게임 결과 저장
    async createGameData(roomId: number) {
        try {
            const room = this.gameRoomList.get(roomId);
            const [winnerUserId, loserUserId] = [
                this.socketUsersService.getPlayerBySocketId(room.socket[room.winner].id).userId,
                this.socketUsersService.getPlayerBySocketId(room.socket[room.loser].id).userId,
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
            await this.gameRepository.save(newGameData);

            //ranking game 후: user table에 exp, level 업데이트
            if (room.gameType === GameType.MATCH) {
                await this.userService.updateUserExp(winnerUserId, newGameData.winnerScore);
                if (room.endGameStatus === GameEndStatus.NORMAL)
                    await this.userService.updateUserExp(loserUserId, newGameData.loserScore);
            }
        } catch (error) {
            console.log('error >> gameService >> createGameData');
            throw new InternalServerErrorException('[ERR] gameService >> createGameData');
        }
    }

    //gameRoom 만들기
    async makeGameRoom(player1Id: string, player2Id: string, gameType: number, gameMode: number) {
        //waitRoom 만들기
        const gameRoom: GameRoom = {
            id: this.gameRoomIdx,
            gameType: gameType,
            gameMode: gameMode,
            gameStatus: GameStatus.WAIT,
            ready: [false, false],
        };
        //TESTCODE
        // console.log('makeGameRoom: ', player1Id, player2Id);
        //방입장
        await this.enterGameRoom(gameRoom, player1Id, player2Id);
        this.gameRoomList.set(this.gameRoomIdx++, gameRoom);
    }

    // players enter the gameRoom
    async enterGameRoom(gameRoom: GameRoom, player1Id: string, player2Id: string) {
        let left: string, right: string;
        if (Math.floor(Math.random() * 2)) {
            left = player1Id;
            right = player2Id;
        } else {
            left = player2Id;
            right = player1Id;
        }
        const leftPlayer = this.socketUsersService.getPlayerBySocketId(left);
        const rightPlayer = this.socketUsersService.getPlayerBySocketId(right);
        gameRoom.socket = [leftPlayer.socket, rightPlayer.socket];
        gameRoom.name = [leftPlayer.userName, rightPlayer.userName];

        //Player 업데이트 && emit('handShake')
        // console.log('enterGameRoom:', gameRoom.id);
        await this.handShake(left, PlayerSide.LEFT, gameRoom.id);
        await this.handShake(right, PlayerSide.RIGHT, gameRoom.id);
    }
    //gameRoom 진입 시 Player 정보 업데이트
    async handShake(playerId: string, side: number, roomId: number): Promise<void> {
        try {
            const player = this.socketUsersService.getPlayerBySocketId(playerId);
            player.roomId = roomId;
            player.side = side;
            player.socket.emit('handShake', { side: side });
            // console.log('handShake done', playerId);
        } catch (error) {
            throw new InternalServerErrorException('[ERR] gameService >> handShake');
        }
    }

    //game 시작 시 => gameRoom 업데이트
    updateGame(gameRoomId: number) {
        this.gameRoomList.get(gameRoomId).gameStatus = GameStatus.GAME;
        this.gameRoomList.get(gameRoomId).startTime = DateTime.now().setZone(TIMEZONE);
        this.gameRoomList.get(gameRoomId).score = [0, 0];
        //TESTCODE: startTime
        // console.log('startTime:', this.gameRoomList.get(gameRoomId).startTime);
    }

    updatePlayer(playerId: string, gameMode: number, gameType: number, friendId?: number | undefined) {
        const player: Player = this.socketUsersService.getPlayerBySocketId(playerId);
        if (player === undefined) throw new BadRequestException('player undefined');
        if (player.roomId) throw new BadRequestException('player already in gameRoom');
        if (friendId) player.friendId = friendId;
        player.gameType = gameType;
        player.gameMode = gameMode;
    }

    //game 각 라운드 시작 시 공 방향 세팅
    getBallStartDir(roomId: number): any {
        const a = Math.round(Math.random()) * 2 - 1; //-1 or 1
        const b = Math.random() * 3 - 1.5; //-1.5 ~ 1.5
        const c = Math.sqrt(a * a + b * b);

        const dirX = a / c;
        const dirZ = b / c;
        this.updateBall(roomId, dirX, dirZ);
        // console.log('ballDirX', dirX, 'ballDirZ', dirZ, '\nballPosX', 0, 'ballPosZ', 0);
        return {
            ballDirX: dirX,
            ballDirY: 0,
            ballDirZ: dirZ,
        };
    }
    //ball 정보 업데이트
    updateBall(roomId: number, dirX: number, dirZ: number, posX?: number, posZ?: number) {
        this.gameRoomList.get(roomId).dirX = dirX;
        this.gameRoomList.get(roomId).dirZ = dirZ;
        this.gameRoomList.get(roomId).posX = posX !== undefined ? posX : 0;
        this.gameRoomList.get(roomId).posZ = posZ !== undefined ? posZ : 0;
    }

    //게임 중단/종료 시 플레이어 리셋
    resetPlayer(player: Player) {
        player.gameType = undefined;
        player.gameMode = undefined;
        player.side = undefined;
        player.roomId = undefined;
        player.friendId = undefined;
    }
    //숫자 보정
    equals(a: number, b: number, tolerance: number = EPSILON): boolean {
        return Math.abs(a - b) <= tolerance;
    }

    //*API용: 프로필 게임 정보
    async getGameHistory(slackId: string) {
        const user: User = await this.userService.getUserBySlackId(slackId);
        if (user === undefined) return null;

        const gameHistory: Array<{
            myScore: number;
            mySide: PlayerSide;
            gameEnd: GameEndStatus;
            rivalName: string;
            rivalSlackId: string;
            rivalScore: number;
            gameType: GameType;
        }> = [];
        const gameRecords = await this.gameRepository.find({
            where: [{ winnerId: user.id }, { loserId: user.id }],
            order: { gameId: 'DESC' },
            take: 5,
        });
        for (const record of gameRecords) {
            let mySide: PlayerSide;
            let rivalId: number;
            if (record.winnerId === user.id) {
                mySide = record.winnerSide;
                rivalId = record.loserId;
            } else {
                mySide = record.loserSide;
                rivalId = record.winnerId;
            }
            const rival: User = await this.userService.findUserById(rivalId);
            if (rival === undefined) throw new BadRequestException('no such rivalId');
            gameHistory.push({
                myScore: mySide === record.winnerSide ? record.winnerScore : record.loserScore,
                mySide: mySide,
                gameEnd: record.endGameStatus,
                rivalName: rival.userName,
                rivalSlackId: rival.slackId,
                rivalScore: mySide === record.winnerSide ? record.loserScore : record.winnerScore,
                gameType: record.gameType,
            });
        }
        return gameHistory;
    }

    async getGameData(slackId: string) {
        try {
            const user: User = await this.userService.getUserBySlackId(slackId);
            if (user === undefined) return null;
            const gameRecords = await this.gameRepository.find({
                where: [
                    { winnerId: user.id, gameType: GameType.MATCH },
                    { loserId: user.id, gameType: GameType.MATCH },
                ],
            });

            const gameData: GameData = {
                normalWin: 0,
                normalLose: 0,
                hardWin: 0,
                hardLose: 0,
            };

            for (const record of gameRecords) {
                if (record.gameMode === GameMode.NORMAL) {
                    if (record.winnerId === user.id) gameData.normalWin += 1;
                    else gameData.normalLose += 1;
                } else {
                    if (record.winnerId === user.id) gameData.hardWin += 1;
                    else gameData.hardLose += 1;
                }
            }
            return gameData;
        } catch (error) {
            console.log('error >> gameService >> getGameData');
            throw new InternalServerErrorException('[ERR] gameService >> getGameData');
        }
    }

    async getFriendGameData(userId: number, friendSlackId: string) {
        try {
            const friend: User = await this.userService.getUserBySlackId(friendSlackId);
            if (friend === undefined) return null;
            const gameRecords = await this.gameRepository.find({
                where: [
                    {
                        winnerId: userId,
                        loserId: friend.id,
                        gameType: GameType.FRIEND,
                    },
                    {
                        winnerId: friend.id,
                        loserId: userId,
                        gameType: GameType.FRIEND,
                    },
                ],
            });

            const friendGameData: FriendGameData = {
                normalWin: 0,
                normalLose: 0,
                hardWin: 0,
                hardLose: 0,
                rightWin: 0,
                rightLose: 0,
                leftWin: 0,
                leftLose: 0,
            };

            for (const record of gameRecords) {
                if (record.gameMode === GameMode.NORMAL) {
                    if (record.winnerId === userId) {
                        friendGameData.normalWin += 1;
                        if (record.winnerSide === PlayerSide.RIGHT) friendGameData.rightWin += 1;
                        else friendGameData.leftWin += 1;
                    } else {
                        friendGameData.normalLose += 1;
                        if (record.loserSide === PlayerSide.RIGHT) friendGameData.rightLose += 1;
                        else friendGameData.leftLose += 1;
                    }
                } else {
                    if (record.winnerId === userId) {
                        friendGameData.hardWin += 1;
                        if (record.winnerSide === PlayerSide.RIGHT) friendGameData.rightWin += 1;
                        else friendGameData.leftWin += 1;
                    } else {
                        friendGameData.hardLose += 1;
                        if (record.loserSide === PlayerSide.RIGHT) friendGameData.rightLose += 1;
                        else friendGameData.leftLose += 1;
                    }
                }
            }
            return friendGameData;
        } catch (error) {
            console.log('error >> gameService >> getFriendGameData');
            throw new InternalServerErrorException('[ERR] gameService >> getFriendGameData');
        }
    }
}
