package kr.magicbox.ssegateway.domain.exception;

import kr.magicbox.ssegateway.global.exception.BusinessException;
import org.springframework.http.HttpStatus;

public class InvalidFieldException extends BusinessException {

    public InvalidFieldException(String message) {
        super(message, HttpStatus.BAD_REQUEST);
    }
}
